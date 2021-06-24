#' Initiate or update the arbitrage database
#'
#' This function creates or updates the past and present arbitrage opportunities
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#' @import cutpointr
#' @import caret
#' @import alabama
#' @import zoo
#' @import ggplot2

arbitrage <- function(path){

	#################################################################################################
	#################################################################################################
	#########      Extract the files
	#################################################################################################
	#################################################################################################


	offers <- list()

	folder_directory <- paste(path, "/MLB_Modeling/Betting/Predicted_Lineups", sep = "")
	path_check <- paste(folder_directory, "/Betting_Database_Pinnacle.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("ERROR: missing file at:", path_check), quote = FALSE)

	}
	offers$pinnacle <- readRDS(path_check)
	offers$pinnacle[, Date := as.Date(Date)]

	#Remove wrong data
	rmv <- offers$pinnacle[Date < as.Date("2021-06-20") & Bet_Type == "POINTS", which = TRUE]
	offers$pinnacle <- offers$pinnacle[-rmv]


	path_check <- paste(folder_directory, "/Betting_Database.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("ERROR: missing file at:", path_check), quote = FALSE)

	}
	offers$lotoQc <- readRDS(path_check)	
	offers$lotoQc <- offers$lotoQc[, names(offers$pinnacle), with = FALSE]

	offers$lotoQc[Bet_Type == "POINTS" & Bet_On == "Below", Bet_Type2 := - Bet_Type2]
	offers$lotoQc[Bet_Type == "SUM" & Bet_On == "Below", Bet_Type2 := - Bet_Type2]
	offers$lotoQc[, Minutes_Until_Start := as.numeric(Minutes_Until_Start)]

	for(i in 1:length(offers)){

		rmv <- offers[[i]][Bet_On == "None" & "Bet_On2" == "None", which = TRUE]
		if(any(rmv)){

			offers[[i]] <- offers[[i]][-rmv]

		}

	}




	#Check if file already exists
	update <- FALSE
	path_save <- paste(folder_directory, "/Arbitrage.rds", sep = "")
	if(file.exists(path_save)){

		update <- TRUE
		old_file <- readRDS(path_save)

		dates_done <- unique(readRDS(path_save)$both$pinnacle$Date)	
		dates_done <- dates_done[which(dates_done != Sys.Date())]

		#Remove today's data from the old file
		for(i in 1:3){

			old_file[[i]] <- lapply(old_file[[i]], function(x){x[Date != Sys.Date()]})

		}

		#Remove old match from the offers
		offers <- lapply(offers, function(x){x[!(Date %in% dates_done)]})

	}








	#Match betting opportunities
	matchups <- lapply(offers, function(x){unique(x[, c("Date", "Team_Home", "Team_Away"), with = FALSE])})
	matchups <- dplyr::inner_join(matchups$pinnacle, matchups$lotoQc)
	data.table::setkeyv(matchups, c("Date", "Team_Home", "Team_Away"))

	for(i in 1:length(offers)){

		data.table::setkeyv(offers[[i]], c("Date", "Team_Home", "Team_Away"))
		offers[[i]] <- offers[[i]][matchups, nomatch = 0]

	}

	#Tag bets per ID
	bets <- unique(offers$pinnacle[, c("Team_Home", "Team_Away", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2", "Inn.", "Date")])
	bets[, ID_bet := c(1:nrow(bets))]
	data.table::setkeyv(bets, c("Team_Home", "Team_Away", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2", "Inn.", "Date"))

	for(i in 1:length(offers)){

		data.table::setkeyv(offers[[i]], c("Team_Home", "Team_Away", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2", "Inn.", "Date"))
		offers[[i]][bets, ID_bet := i.ID_bet]

	}


	offers$lotoQc <- offers$lotoQc[bets, nomatch = 0]
	offers$lotoQc <- offers$lotoQc[, names(offers$pinnacle), with = FALSE]

	temp <- offers$pinnacle[offers$lotoQc, nomatch = 0]
	temp <- temp[, names(offers$lotoQc), with = FALSE]
	offers$pinnacle <- offers$pinnacle[, names(offers$lotoQc), with = FALSE]

	offers$pinnacle <- data.table::copy(rbind(offers$pinnacle[Bet_Type == "WINNER" & Bet_On == "None"],
								temp))

	temp <- NULL

	offers <- lapply(offers, unique)

	#Remove potential duplicates
	for(i in 1:length(offers)){

		offers[[i]][, Retain := lapply(.SD, function(x){

			out <- rep(FALSE, length(x))
			out[which.min(x)] <- TRUE
			return(out)

			}), by = c("ID_bet", "Scrapping_Time"), .SDcols = "Factor"]

		offers[[i]] <- offers[[i]][Retain == TRUE]
		offers[[i]][, Retain := NULL]

	}


	#Compute implied fair probabilities
	output <- list()

	id <- 1

	#WINNER
	temp <- lapply(offers, function(x){x[Bet_Type == "WINNER"]})
	for(i in 1:length(temp)){

		combinations <- unique(temp[[i]][, c("Date", "Team_Home", "Team_Away", "Inn.", "Scrapping_Time"), with = FALSE])
		data.table::setkeyv(combinations, c("Date", "Team_Home", "Team_Away", "Inn.", "Scrapping_Time"))
		data.table::setkeyv(temp[[i]], c("Date", "Team_Home", "Team_Away", "Inn.", "Scrapping_Time"))

		for(j in 1:nrow(combinations)){

			prob_space <- temp[[i]][.(combinations[j])]

			if(nrow(prob_space) < 2 | length(unique(prob_space$Scrapping_Time)) > 1){

				next

			}

			#Reciprocal of offered rates
			F <- prob_space$Factor

			if(length(F) > 2){

				g <- function(e){(sum(1 / (F + e)) - 1)^2}
				margin <- optim(0.1, g, method = "Brent", lower = 0, upper = 1)$par

			} else {

				b <- sum(F) - 2
				c <- prod(F) - sum(F)
				margin <- (-b + sqrt(b^2 - 4*c)) / 2

			}

			margin_geom <- sum(1 / F)

			index <- temp[[i]][.(combinations[j]), which = TRUE]

			temp[[i]][index, P1 := 1 / (F + margin)]
			temp[[i]][index, P2 := (1 / margin_geom) * 1 / F]

			temp[[i]][index, F1 := 1 / P1]
			temp[[i]][index, F2 := 1 / P2]	

			temp[[i]][index, M1 := margin]
			temp[[i]][index, M2 := 1 - (1 / margin_geom)]	

			m <- nrow(prob_space)
			if(i == 1){

				temp[[i]][index, ID_bet2 := seq(from = id, to = id + m - 1, by = 1)]
				id <- id + m

			}

			if(m > 2){

				temp[[i]][index, Has_Ties := TRUE]

			} else {

				temp[[i]][index, Has_Ties := FALSE]

			}

			if(margin > 0.5){break}

		}

	}

	output$pinnacle <- temp$pinnacle[!is.na(P1)]
	output$lotoQc <- temp$lotoQc[!is.na(P1)]


	#POINTS
	temp <- lapply(offers, function(x){x[Bet_Type == "POINTS"]})
	for(i in 1:length(temp)){

		combinations <- unique(temp[[i]][, c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Bet_On2", "Scrapping_Time"), with = FALSE])
		data.table::setkeyv(combinations, c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Bet_On2", "Scrapping_Time"))
		data.table::setkeyv(temp[[i]], c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Bet_On2", "Scrapping_Time"))

		for(j in 1:nrow(combinations)){

			prob_space <- temp[[i]][.(combinations[j])]

			if(nrow(prob_space) < 2 | length(unique(prob_space$Scrapping_Time)) > 1){

				next

			}


			#Reciprocal of offered rates
			F <- prob_space$Factor

			if(length(F) > 2){

				g <- function(e){(sum(1 / (F + e)) - 1)^2}
				margin <- optim(0.1, g, method = "Brent", lower = 0, upper = 1)$par

			} else {

				b <- sum(F) - 2
				c <- prod(F) - sum(F)
				margin <- (-b + sqrt(b^2 - 4*c)) / 2

			}

			margin_geom <- sum(1 / F)

			index <- temp[[i]][.(combinations[j]), which = TRUE]

			temp[[i]][index, P1 := 1 / (F + margin)]
			temp[[i]][index, P2 := (1 / margin_geom) * 1 / F]

			temp[[i]][index, F1 := 1 / P1]
			temp[[i]][index, F2 := 1 / P2]	

			temp[[i]][index, M1 := margin]
			temp[[i]][index, M2 := 1 - (1 / margin_geom)]	

			m <- nrow(prob_space)
			if(i == 1){

				temp[[i]][index, ID_bet2 := seq(from = id, to = id + m - 1, by = 1)]
				id <- id + m

			}


		}

		temp[[i]][, Has_Ties := FALSE]

	}

	for(i in 1:length(temp)){

		output[[i]] <- rbind(output[[i]], temp[[i]][!is.na(P1)])

	}



	#SUMS
	temp <- lapply(offers, function(x){x[Bet_Type == "SUM"]})
	for(i in 1:length(temp)){

		combinations <- unique(temp[[i]][, c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Scrapping_Time"), with = FALSE])
		data.table::setkeyv(combinations, c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Scrapping_Time"))
		data.table::setkeyv(temp[[i]], c("Date", "Team_Home", "Team_Away", "Inn.", "Bet_Type2", "Scrapping_Time"))

		for(j in 1:nrow(combinations)){

			prob_space <- temp[[i]][.(combinations[j])]

			if(nrow(prob_space) < 2 | length(unique(prob_space$Scrapping_Time)) > 1){

				next

			}


			#Reciprocal of offered rates
			F <- prob_space$Factor

			if(length(F) > 2){

				g <- function(e){(sum(1 / (F + e)) - 1)^2}
				margin <- optim(0.1, g, method = "Brent", lower = 0, upper = 1)$par

			} else {

				b <- sum(F) - 2
				c <- prod(F) - sum(F)
				margin <- (-b + sqrt(b^2 - 4*c)) / 2

			}

			margin_geom <- sum(1 / F)

			index <- temp[[i]][.(combinations[j]), which = TRUE]

			temp[[i]][index, P1 := 1 / (F + margin)]
			temp[[i]][index, P2 := (1 / margin_geom) * 1 / F]

			temp[[i]][index, F1 := 1 / P1]
			temp[[i]][index, F2 := 1 / P2]	

			temp[[i]][index, M1 := margin]
			temp[[i]][index, M2 := 1 - (1 / margin_geom)]	

			m <- nrow(prob_space)
			if(i == 1){

				temp[[i]][index, ID_bet2 := seq(from = id, to = id + m - 1, by = 1)]
				id <- id + m

			}

		}

		temp[[i]][, Has_Ties := FALSE]

	}

	for(i in 1:length(temp)){

		output[[i]] <- rbind(output[[i]], temp[[i]][!is.na(P1)])

	}


	#SPREAD
	for(i in 1:length(offers)){

		offers[[i]][, Dummy := Bet_Type2]
		offers[[i]][Bet_Type == "SPREAD" & Bet_On2 == "Away", Dummy := -Dummy]

	}


	temp <- lapply(offers, function(x){data.table::copy(x[Bet_Type == "SPREAD"])})
	for(i in 1:length(temp)){

		combinations <- unique(temp[[i]][, c("Date", "Team_Home", "Team_Away", "Inn.", "Dummy", "Scrapping_Time"), with = FALSE])
		data.table::setkeyv(combinations, c("Date", "Team_Home", "Team_Away", "Inn.", "Dummy", "Scrapping_Time"))
		data.table::setkeyv(temp[[i]], c("Date", "Team_Home", "Team_Away", "Inn.", "Dummy", "Scrapping_Time"))

		for(j in 1:nrow(combinations)){

			prob_space <- temp[[i]][.(combinations[j])]

			if(nrow(prob_space) < 2 | length(unique(prob_space$Scrapping_Time)) > 1){

				next

			}


			#Reciprocal of offered rates
			F <- prob_space$Factor

			if(length(F) > 2){

				g <- function(e){(sum(1 / (F + e)) - 1)^2}
				margin <- optim(0.1, g, method = "Brent", lower = 0, upper = 1)$par

			} else {

				b <- sum(F) - 2
				c <- prod(F) - sum(F)
				margin <- (-b + sqrt(b^2 - 4*c)) / 2

			}

			margin_geom <- sum(1 / F)

			index <- temp[[i]][.(combinations[j]), which = TRUE]

			temp[[i]][index, P1 := 1 / (F + margin)]
			temp[[i]][index, P2 := (1 / margin_geom) * 1 / F]

			temp[[i]][index, F1 := 1 / P1]
			temp[[i]][index, F2 := 1 / P2]	

			temp[[i]][index, M1 := margin]
			temp[[i]][index, M2 := 1 - (1 / margin_geom)]	

			m <- nrow(prob_space)
			if(i == 1){

				temp[[i]][index, ID_bet2 := seq(from = id, to = id + m - 1, by = 1)]
				id <- id + m

			}

		}

		temp[[i]][, Has_Ties := FALSE]

	}

	for(i in 1:length(temp)){

		offers[[i]][, Dummy := NULL]
		temp[[i]][, Dummy := NULL]
		output[[i]] <- rbind(output[[i]], temp[[i]][!is.na(P1)])

	}

	data.table::setkeyv(output$pinnacle, c("Team_Home", "Team_Away", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2", "Inn.", "Date"))
	data.table::setkeyv(output$lotoQc, c("Team_Home", "Team_Away", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2", "Inn.", "Date"))

	output$pinnacle[, Website := "Pinnacle"]
	output$lotoQc[, Website := "LotoQc"]


	#Keep common bets
	ID <- unique(output$pinnacle[Has_Ties == FALSE]$ID_bet)
	ID <- intersect(ID, unique(output$lotoQc$ID_bet))

	for(i in 1:length(output)){

		output[[i]] <- output[[i]][ID_bet %in% ID]

	}


	output$intersect <- unique(output$pinnacle, by = "ID_bet")

	#Adds realizations
	scores <- data.table::fread(paste(path, "/MLB_Modeling/Scores/Clean_Data/FanGraphs_Scores.csv", sep = ""))		
	scores[, Date := as.Date(Date)]

	#Remove instances where 2 matches were played on the same day
	scores[, Is_Unique := lapply(.SD, function(x){length(x) == 1}), by = c("Date", "Team_Home", "Team_Away"), .SDcols = "ID"]
	scores <- scores[Is_Unique == TRUE]
	scores[, Is_Unique := NULL]


	data.table::setkeyv(scores, c("Date", "Team_Home", "Team_Away"))
	for(i in 1:length(output)){

		data.table::setkeyv(output[[i]], c("Date", "Team_Home", "Team_Away"))
		output[[i]][scores, ID := i.ID]

	}

	output$results <- output$intersect[!is.na(ID)]
	output$intersect <- NULL 

	if(nrow(output$results) > 0){

		seasons <- unique(stringr::str_split(output$results$Date, "-", simplify = TRUE)[, 1])

		play_by_play <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/Play_by_play_Database.rds", sep = ""))
		play_by_play <- play_by_play[seasons]

		play_by_play <- data.table::copy(dplyr::bind_rows(play_by_play))
		IDs <- unique(output$results$ID)
		play_by_play <- play_by_play[ID %in% IDs]


		play_by_play[Inn. > 9, Inn. := 9]
		play_by_play[, Is_Last := lapply(.SD, function(x){

			out <- rep(FALSE, length(x))
			out[length(x)] <- TRUE
			return(out)


			}), by = c("ID", "Inn."), .SDcols = "Score_Home"]

		play_by_play <- play_by_play[Is_Last == TRUE, c("ID", "Inn.", "Score_Home", "Score_Away")]
		play_by_play <- play_by_play[Inn. %in% unique(output$results$Inn.)]

		play_by_play[, Sum := Score_Home + Score_Away]
		play_by_play[, Spread := Score_Home - Score_Away]

		data.table::setkeyv(output$results, c("ID", "Inn."))
		data.table::setkeyv(play_by_play, c("ID", "Inn."))

		#WINNER
		index <- output$results[Bet_Type == "WINNER" & Bet_On2 == "Home", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Home > i.Score_Away]
		output$results[index, Outcome := temp$Outcome]


		index <- output$results[Bet_Type == "WINNER" & Bet_On2 == "Away", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Away > i.Score_Home]
		output$results[index, Outcome := temp$Outcome]	

		#SPREAD
		index <- output$results[Bet_Type == "SPREAD" & Bet_On2 == "Home", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Spread > -Bet_Type2]
		output$results[index, Outcome := temp$Outcome]

		index <- output$results[Bet_Type == "SPREAD" & Bet_On2 == "Away", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := -i.Spread > -Bet_Type2]
		output$results[index, Outcome := temp$Outcome]

		#Points
		index <- output$results[Bet_Type == "POINTS" & Bet_On2 == "Home" & Bet_On == "Above", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Home > Bet_Type2]
		output$results[index, Outcome := temp$Outcome]


		index <- output$results[Bet_Type == "POINTS" & Bet_On2 == "Home" & Bet_On == "Below", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Home < Bet_Type2]
		output$results[index, Outcome := temp$Outcome]


		index <- output$results[Bet_Type == "POINTS" & Bet_On2 == "Away" & Bet_On == "Above", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Away > Bet_Type2]
		output$results[index, Outcome := temp$Outcome]


		index <- output$results[Bet_Type == "POINTS" & Bet_On2 == "Away" & Bet_On == "Below", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Score_Away < Bet_Type2]
		output$results[index, Outcome := temp$Outcome]


		#SUMS
		index <- output$results[Bet_Type == "SUM" & Bet_On == "Above", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Sum > Bet_Type2]
		output$results[index, Outcome := temp$Outcome]

		index <- output$results[Bet_Type == "SUM" & Bet_On == "Below", which = TRUE]
		temp <- output$results[index]
		temp[play_by_play, Outcome := i.Sum < Bet_Type2]
		output$results[index, Outcome := temp$Outcome]

		data.table::setkey(output$results, "ID_bet")
		for(i in 1:2){

			data.table::setkey(output[[i]], "ID_bet")
			output[[i]][output$results, Outcome := i.Outcome]

			output[[i]][, Most_Recent := lapply(.SD, function(x){

				out <- rep(FALSE, length(x))
				out[which.max(x)] <- TRUE
				return(out)

			}), by = "ID_bet", .SDcols = "Scrapping_Time"]

		}


	} else {

		for(i in 1:length(output)){

			output[[i]][, Outcome := NA]

		}

	}

	output$results <- NULL

	output$past <- list(pinnacle = data.table::copy(output$pinnacle[!is.na(Outcome)]),
							lotoQc = data.table::copy(output$lotoQc[!is.na(Outcome)]))

	output$present <- list(pinnacle = data.table::copy(output$pinnacle[is.na(Outcome)]),
							lotoQc = data.table::copy(output$lotoQc[is.na(Outcome)]))

	output$both <- list(pinnacle = data.table::copy(output$pinnacle),
							lotoQc = data.table::copy(output$lotoQc))

	output$pinnacle <- NULL
	output$lotoQc <- NULL

	for(i in 1:length(output$past)){

		output$past[[i]][Outcome == TRUE, Result := "Win"]
		output$past[[i]][Outcome == FALSE, Result := "Loss"]
		output$past[[i]][, Result := as.factor(Result)]

		output$present[[i]][, Outcome := NULL]

	}





	#--------------------------------
	#Arbitrage

	#Add the most recently computed odds to the pinnacle dataframes
	#I.e.: the latest estimate for P(Event occurs) = 1 / (Fair return rate)
	for(i in 1:length(output)){

		if(nrow(output[[i]]$pinnacle) == 0){

			to_fill <- c("P1_Arb", "P2_Arb", "Scrapping_Time_Arb", "E_R1", "E_R2", "P1_Arb", "P2_Arb")
			for(x in to_fill){

				output[[i]]$pinnacle[, (x) := NA]

			}
		
			next

		}

		data.table::setkey(output[[i]]$lotoQc, "ID_bet")

		for(j in 1:nrow(output[[i]]$pinnacle)){

			id <- output[[i]]$pinnacle$ID_bet[j]
			timestamp <- output[[i]]$pinnacle$Scrapping_Time[j]

			avaible <- output[[i]]$lotoQc[.(id)]
			avaible <- avaible[Scrapping_Time <= timestamp]

			if(nrow(avaible) == 0){next}

			output[[i]]$pinnacle[j, c("P1_Arb", "P2_Arb", "Scrapping_Time_Arb") := avaible[which.max(Scrapping_Time), c("P1", "P2", "Scrapping_Time")]]
			output[[i]]$pinnacle[j, Time_Diff := Scrapping_Time - Scrapping_Time_Arb]

		}

		output[[i]]$arbitrage <- output[[i]]$pinnacle[!is.na(P1_Arb)]
		output[[i]]$arbitrage[, E_R1 := P1_Arb * Factor - 1]
		output[[i]]$arbitrage[, E_R2 := P2_Arb * Factor - 1]

		output[[i]]$arbitrage <- output[[i]]$arbitrage[E_R1 > 0.001 | E_R2 > 0.001]

		output[[i]]$arbitrage[, Var_R1 := P1_Arb * (1 - P1_Arb) * Factor^2]
		output[[i]]$arbitrage[, Var_R2 := P2_Arb * (1 - P2_Arb) * Factor^2]	

	}



	#--------------------------------
	#Arbitrage, Pinnacle
	output$arbitrage_results <- list(

		equal = output$both$arbitrage[E_R1 >= 0.001 & Website == "Pinnacle"] %>%
					.[!is.na(Outcome), R := Factor * Outcome - 1] %>%
					.[, P := P1_Arb] %>%
					.[, Var := Var_R1] %>%
					.[, E_R := E_R1],

		proportional = output$both$arbitrage[E_R2 >= 0.001 & Website == "Pinnacle"] %>%
					.[!is.na(Outcome), R := Factor * Outcome - 1] %>%
					.[, P := P2_Arb] %>%
					.[, Var := Var_R2] %>%
					.[, E_R := E_R2]


		)

	output$arbitrage_results$mixed <- output$both$arbitrage
	output$arbitrage_results$mixed[, P := (P1_Arb + P2_Arb)/2] 
	output$arbitrage_results$mixed[, E_R := P * Factor - 1]
	output$arbitrage_results$mixed <- output$arbitrage$mixed[E_R >= 0.001 & Website == "Pinnacle"]

	output$arbitrage_results$mixed[!is.na(Outcome), R := Factor * Outcome - 1]
	output$arbitrage_results$mixed[, Var := Factor^2 *  P * (1 - P)]


	#-----------------------------
	#Keep the first instance of a favourable bet
	for(i in 1:length(output$arbitrage_results)){

		output$arbitrage_results[[i]][, First_Opportunity := lapply(.SD, function(x){

			out <- rep(FALSE, length(x))
			out[which.min(x)] <- TRUE
			return(out)

		}), by = "ID_bet", .SDcols = "Scrapping_Time"]

		output$arbitrage_results[[i]] <- output$arbitrage_results[[i]][First_Opportunity == TRUE]
		output$arbitrage_results[[i]][, First_Opportunity := NULL]

	}

	#-----------------------------
	#Remove
	#Keep a single bet per category
	#for(i in 1:length(output$arbitrage_results)){

		#output$arbitrage_results[[i]][, Sharpe := E_R / sqrt(Var)]
		#output$arbitrage_results[[i]][, Best_Per_Cat := lapply(.SD, function(x){

			#out <- rep(FALSE, length(x))
			#out[which.max(x)] <- TRUE
			#return(out)

		#}), by = c("Bet_Type", "ID", "Inn."), .SDcols = "Sharpe"]

		#output$arbitrage_results[[i]] <- output$arbitrage_results[[i]][Best_Per_Cat == TRUE]
		#output$arbitrage_results[[i]][, Best_Per_Cat := NULL]

	#}	


	#-----------------------------
	#Choose the most recent scrapping time to place bets
	#I.e.: ignore opportunities that were avaible solely in the past
	#
	# + Remove arbirtrage opportunities with a time-gap
	for(i in 1:length(output$arbitrage_results)){

		if(nrow(output$arbitrage_results[[i]]) > 0){

			output$arbitrage_results[[i]][, Keep := lapply(.SD, function(x){x == max(x)}), by = c("Date"), .SDcols = "Scrapping_Time"]
			output$arbitrage_results[[i]] <- output$arbitrage_results[[i]][Keep == TRUE]
			output$arbitrage_results[[i]][, Keep := NULL]

			output$arbitrage_results[[i]][, Time_Diff := (Scrapping_Time - Scrapping_Time_Arb) / 60]
			output$arbitrage_results[[i]] <- output$arbitrage_results[[i]][Time_Diff == 0]

		} else {

			output$arbitrage_results[[i]][, Time_Diff = 0]

		}


	}		




	#--------------------------------
	#Optimal bets

	#Load Bernoulli CovMat to estimate the covariance between bets
	path_load2 <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Bernoulli_CovMat.rds", sep = "")
	Bernoulli_CovMat <- readRDS(path_load2)


	for(i in 1:length(output$arbitrage_results)){

		if(nrow(output$arbitrage_results[[i]]) == 0){next}

		data.table::setorderv(output$arbitrage_results[[i]], c("Date", "Game_Time"))
		dates <- sort(unique(output$arbitrage_results[[i]]$Date))

		if(update){

			start_capital <- old_file$arbitrage_results[[i]][!is.na(Capital)]$Capital
			start_capital <- start_capital[length(start_capital)]
			capital <- start_capital

		} else {

			start_capital <- 10000
			capital <- 10000

		}

		
		for(j in 1:length(dates)){

			index <- data.table::copy(output$arbitrage_results[[i]][Date == dates[j], which = TRUE])

			if(length(index) == 0){

				next

			}

			temp <- output$arbitrage_results[[i]][index]
			temp[, tag := c(1:nrow(temp))]

			matchups <- unique(temp[, c("Team_Home", "Team_Away"), with = FALSE])
			data.table::setkeyv(matchups, c("Team_Home", "Team_Away"))
			data.table::setkeyv(temp, c("Team_Home", "Team_Away"))

			vcov_matrices <- list()
			for(r in 1:nrow(matchups)){

				subindex <- temp[matchups[r], which = TRUE]
				temp2 <- temp[subindex]	

				#bet identifier for Bernoulli_CovMat
				identifiers <- rep(NA, nrow(temp2))
				for(k in 1:length(identifiers)){

					if(temp2$Bet_Type[k] == "WINNER"){

						identifiers[k] <- paste("WINNER", temp2$Bet_On2[k])

					} else if(temp2$Bet_Type[k] == "SPREAD"){

						identifiers[k] <- paste("SPREAD", temp2$Bet_Type2[k], temp2$Bet_On2[k])

					} else if(temp2$Bet_Type[k] == "SUM"){

						identifiers[k] <- paste("SUM", temp2$Bet_On[k],  temp2$Bet_Type2[k])

					} else {

						identifiers[k] <- paste("POINTS", temp2$Bet_On[k],  temp2$Bet_Type2[k], temp2$Bet_On2[k])

					}

				}

				identifiers <- paste(temp2$Inn., identifiers)

				if(nrow(temp2) == 1){

					vcov_matrices[[r]] <- as.matrix(temp2$Var[1])
					colnames(vcov_matrices[[r]]) <- identifiers[1]
					next

				}

				index2 <- match(identifiers, colnames(Bernoulli_CovMat))
				rmv <- which(is.na(index2))

				if(any(rmv)){

					temp <- temp[-subindex[rmv]]
					temp2 <- temp2[-rmv]
					index2 <- index2[-rmv]
					identifiers <- identifiers[-rmv]

				}

				if(length(identifiers) == 0){

					next

				}
				
				#Build the covariance matrix
				Bernoulli_CovMat_subset <- Bernoulli_CovMat[index2, index2]

				#Variances for the diagonal elements
				variances <- temp2$Var

				#probability vector
				p <- temp2$P

				#Factor vector
				F <- temp2$Factor

				#Estimate for the covariances
				#E[I_1 * I_2] = alpha * sqrt(E[I_1] * E[I_2])
				vcov_mat <- p %*% t(p) * Bernoulli_CovMat_subset - p %*% t(p)
				diag(vcov_mat) <- p * (1 - p)
				vcov_mat <- vcov_mat * (F %*% t(F))

				if(length(vcov_mat) == 1){

					vcov_mat <- as.matrix(vcov_mat)
					colnames(vcov_mat) <- identifiers

				}

				vcov_matrices[[r]] <- vcov_mat

			}

			vcov_matrices <- vcov_matrices[which(!unlist(lapply(vcov_matrices, is.null)))]

			vcov_mat <- matrix(0, nrow(temp), nrow(temp))
			from <- 1
			for(a in 1:length(vcov_matrices)){

				to <- from + nrow(vcov_matrices[[a]]) - 1
				b <- c(from:to)
				vcov_mat[b,b] <- vcov_matrices[[a]]

				from <- to + 1

			}

			colnames(vcov_mat) <- unlist(lapply(vcov_matrices, colnames))
			rownames(vcov_mat) <- colnames(vcov_mat)

			#returns vector
			mu <- temp$E_R

			#Add the r = 0, var = 0 option
			mu <- c(0, mu)
			vcov_mat <- rbind(rep(0, nrow(vcov_mat)+1), cbind(rep(0, nrow(vcov_mat)), vcov_mat))
			rownames(vcov_mat)[1] <- "No Bet"
			colnames(vcov_mat)[1] <- "No Bet"


			#Power series approximation 
			f_to_max <- function(w){

				log(1 + w %*% mu) - 
				(1/2) * t(w) %*% vcov_mat %*% w / as.numeric((1 + w %*% mu)^2)

			}

			f_to_min <- function(w){-as.numeric(f_to_max(w))}

			#gradient
			g_f_to_max <- function(w){

				mu / as.numeric(1 + w %*% mu) - 
				vcov_mat %*% w / as.numeric((1 + w %*% mu)^2) + mu * as.numeric(t(w) %*% vcov_mat %*% w) / as.numeric((1 + w %*% mu)^3)		

			}

			g_f_to_min <- function(w){-g_f_to_max(w)}


			#Sum(weights) - 1 = 0
			heq <- function(w){sum(w) - 1}
			heq.jac <- function(w){matrix(1, 1, length(w))}

			hin <- function(w){w}
			hin.jac <- function(w){diag(rep(1, length(w)))}


			c(1, rep(0, length(mu) - 1))

			optim_sol <- alabama::auglag(par = rep(1, length(mu)) / length(mu),
										fn = f_to_min,
										gr = g_f_to_min,
										heq = heq,
										heq.jac = heq.jac,
										hin = hin,
										hin.jac = hin.jac,
										control.outer = list(method="nlminb"))

			output$arbitrage_results[[i]][index, Weights := 0]

			index <- index[temp$tag]

			pars <- optim_sol$par
			pars <- sapply(pars, function(x){max(x, 0)})

			#Min 1$ bet
			too_small <- which(capital * pars < 1)
			if(any(too_small)){

				pars[too_small] <- 0

			}

			pars <- pars / sum(pars)

			output$arbitrage_results[[i]][index, Weights := pars[-1]]
			output$arbitrage_results[[i]][index, Bet := round(Weights * capital, 2)]


			output$arbitrage_results[[i]][index, Mean_Abs_Lambda_Diff := mean(abs(as.numeric(optim_sol$par %*% optim_sol$gradient) - optim_sol$gradient))]
			output$arbitrage_results[[i]][index, Optim_Val := -optim_sol$value]


			#0.95 CI via bootstrap
			CI <- as.numeric(pars %*% mu) + c(-1, 1) * qnorm(0.975) * sqrt(as.numeric(t(pars) %*% vcov_mat %*% pars))
			CI <- capital + capital * CI

			invested <- 1 - pars[1]

			output$arbitrage_results[[i]][index, Capital_95_lb := CI[1]]
			output$arbitrage_results[[i]][index, Capital_95_ub := CI[2]]
			output$arbitrage_results[[i]][index, E_Capital := (1 - invested) * capital  + invested * capital * as.numeric(1 + pars %*% mu)]

			sharpe <- as.numeric(pars %*% mu / sqrt(t(pars) %*% vcov_mat %*% pars))
			output$arbitrage_results[[i]][index, Sharpe_Weighted := sharpe]

			#Add results if possible
			if(!any(is.na(output$arbitrage_results[[i]]$R[index]))){

				output$arbitrage_results[[i]][index, Profits := R * Bet]
				capital <- capital + sum(output$arbitrage_results[[i]][index]$Profits)

			}

		}

		output$arbitrage_results[[i]] <- output$arbitrage_results[[i]][!is.na(Optim_Val)]

		past_index <- which(!is.na(output$arbitrage_results[[i]]$R))

		output$arbitrage_results[[i]][, Weights_Total := lapply(.SD, sum), by = c("Date"), .SDcols = "Weights"]
		output$arbitrage_results[[i]][, Bet_Total := lapply(.SD, sum), by = c("Date"), .SDcols = "Bet"]

		if(any(past_index)){

			output$arbitrage_results[[i]][past_index, Profits_Total := lapply(.SD, sum), by = c("Date"), .SDcols = "Profits"]
			output$arbitrage_results[[i]][past_index, Geom_Return_Daily := 1 + Weights_Total * Profits_Total/Bet_Total]
			
			output$arbitrage_results[[i]][past_index, Capital := start_capital + cumsum(Profits[past_index])]

		}

	}


	#Add backs the old file
	for(i in 1:length(output)){

		for(j in 1:length(output[[i]])){

			if(nrow(output[[i]][[j]]) == 0){

				output[[i]][[j]] <- old_file[[i]][[j]]
				next

			}


			must_contain <- names(old_file[[i]][[j]])
			missing <- which(!must_contain %in% names(output[[i]][[j]]))

			if(any(missing)){

				for(x in names(old_file[[i]][[j]])[missing]){

					output[[i]][[j]][, (x) := NA]

				}
				
			}

			output[[i]][[j]] <- rbind(old_file[[i]][[j]], output[[i]][[j]])


		}

	}



	#Performance metric
	#--------------------------------
	webst <- c("Pinnacle", "LotoQc")
	metrics <- list()

	for(i in 1:length(output$past)){

		temp <- output$past[[i]][Most_Recent == TRUE]

		#Use estimate with best log-loss
		logloss1 <- abs(mean(log(temp$P1) * temp$Outcome + log(1 - temp$P1) * !temp$Outcome))
		logloss2 <- abs(mean(log(temp$P2) * temp$Outcome + log(1 - temp$P2) * !temp$Outcome))

		best <- "P1"
		if(logloss2 < logloss1){

			best <- "P2"

		}

		p <- temp[, best, with = FALSE][[1]]
		cutoff <- cutpointr::cutpointr(p, temp$Outcome, method = cutpointr::maximize_metric, metric = cutpointr::sum_sens_spec)

		preds <- rep("Win", length(p))
		preds[which(p < cutoff$optimal_cutpoint)] <- "Loss"
		preds <- as.factor(preds)

		CM <- caret::confusionMatrix(preds, temp$Result, positive = "Win")

		metrics[[i]] <- list(best_estimator = best,
								cutpoint = cutoff,
								confusion_mat = CM,
								AUC = cutoff$AUC,
								LogLoss = min(logloss1, logloss2))


	}

	names(metrics) <- webst


	#Graph the evolution of the capital
	graphs <- list()
	for(i in 1:length(output$arbitrage_results)){


		frame <- data.table::copy(output$arbitrage_results[[i]][!is.na(R), c("Capital", "Capital_95_lb", "Capital_95_ub"), with = FALSE])
		frame[, N := c(1:nrow(frame))]


		graphs[[i]] <- ggplot2::ggplot(data = frame) + ggplot2::geom_line(aes(x = N, y = Capital, color = "Capital"), linetype = "solid", size = 1.5) +
												ggplot2::geom_line(aes(x = N, y = Capital_95_lb, color = "#FC4E07"), linetype = "dashed", size = 0.75) +
												ggplot2::geom_line(aes(x = N, y = Capital_95_ub, color = "#FC4E07"), linetype = "dashed", size = 0.75) +
												ggplot2::geom_hline(aes(yintercept = 10000, color = "#E7B800"), linetype = "dashed", size = 0.75) +
												ggplot2::scale_color_manual(values = c("#E7B800", "#FC4E07", "#00AFBB"),
																	labels = c("Starting Capital", "95% Confidence Interval", "Current Capital")) +
												ggplot2::scale_linetype_manual(values = c("dashed", "solid")) +
												ggplot2::labs(color = "Legend") +
												ggplot2::xlab("Number of bets taken") +
												ggplot2::ylab("Value") 

	}

	names(graphs) <- names(output$arbitrage_results)


	output$metrics <- metrics
	output$graphs <- graphs
	
	#SAVE

	saveRDS(output, path_save)


}