#' Updating the score and player data
#'
#' This function updates the score and player statistics R databases.
#'
#' @param path the path used by the webscrapper to store data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'
update_database <- function(path){


	################################################
	################ INITIALISATION ################
	################################################

	options(scipen=999)

	################################################
	################################################


	################################################
	################ RETRIEVE DATA  ################
	################################################

	suffix <- c("/MLB_Modeling/Scores/Clean_Data/FanGraphs_Scores.csv", 
				"/MLB_Modeling/Bat/Clean_Data/FanGraphs_Box_Scores.csv",
				"/MLB_Modeling/Pitch/Clean_Data/FanGraphs_Box_Scores.csv",
				"/MLB_Modeling/Bat/Clean_Data/Lineups_BR.csv",
				"/MLB_Modeling/Pitch/Clean_Data/Lineups_BR.csv")

	data <- list()
	for(sf in suffix){

		if(sf == suffix[2] | sf == suffix[3]){

			#Load
			data[[length(data) + 1]] <- data.table::fread(paste(path, sf, sep = ""))
			#Format date
			date_cols <- c("Date")
	 		data.table::setDT(data[[length(data)]])[, (date_cols) := lapply(.SD, anytime::anydate), .SDcols = date_cols]

	 		#Add seasons
	 		season <- stringr::str_split(data[[length(data)]][, (Date)], "-", simplify = TRUE)[, 1]
	 		season <- as.data.frame(as.numeric(season))
	 		colnames(season) <- "Season"

	 		data[[length(data)]] <- cbind(data[[length(data)]], data.table::as.data.table(season))

	 		#Order dataframe
	 		data[[length(data)]] <- data[[length(data)]][order(Season, Team, Date)]

	 		#Remove NaN indicator columns
	 		keep <- which(sapply(colnames(data[[length(data)]]), function(x){!grepl("NaN", x, fixed = TRUE)})) 		
			data[[length(data)]] <- data[[length(data)]][, ..keep]

			#Remove Position columns
	 		keep <- which(sapply(colnames(data[[length(data)]]), function(x){!grepl("Position", x, fixed = TRUE)})) 		
			data[[length(data)]] <- data[[length(data)]][, ..keep]		

			#Order columns
			#Split between numerical variables, and characters / factors / dates
			col_types <- unlist(sapply(data[[length(data)]], class))
			rmv <- which(names(col_types) == "Date2")
			col_types <- col_types[-rmv]
			fix <- which(names(col_types) == "Date1")
			names(col_types)[fix] <- "Date"

			col_names <- colnames(data[[length(data)]])

			index_numeric <- which((col_types == "numeric" | col_types == "integer") & (col_names != "Season" & col_names != "ID"))
			index_numeric <- index_numeric[order(colnames(data[[length(data)]])[index_numeric])]

			index_nm <- c(1:ncol(data[[length(data)]]))[-index_numeric]
			index_nm <- index_nm[order(colnames(data[[length(data)]])[index_nm])]

			index <- c(index_numeric, index_nm)
			data[[length(data)]] <- data[[length(data)]][, ..index]

		} else {

			#Load
			data[[length(data) + 1]] <- data.table::fread(paste(path, sf, sep = ""))
			#Format date
			date_cols <- c("Date")
	 		data.table::setDT(data[[length(data)]])[, (date_cols) := lapply(.SD, anytime::anydate), .SDcols = date_cols]

	 		#Add seasons
	 		season <- stringr::str_split(data[[length(data)]][, (Date)], "-", simplify = TRUE)[, 1]
	 		season <- as.data.frame(as.numeric(season))
	 		colnames(season) <- "Season"

	 		data[[length(data)]][, Season := season]

	 		#Order dataframe
	 		data[[length(data)]] <- data[[length(data)]][order(Season, Date)]		

		}


	}
	names(data) <- c("Scores", "Bat", "Pitch", "Bat_Lineups", "Pitch_Lineups")

	for(i in 2:3){

		data[[i]][, Announced := "No"]

	}

	data$Scores[, Win := as.numeric(data$Scores$Score_Home > data$Scores$Score_Away)]

	ID <- data$Scores$ID

	#Retrieve moneyline frame 
	ML <- data.table::fread(paste(path, "/MLB_Modeling/Betting/Clean_Data/MLB_Odds.csv", sep = ""))
	ML <- ML[ID != -1]

	ML[, Season := as.numeric(stringr::str_split(ML$Date, "-", simplify = TRUE)[, 1])]




	#Complete the lineup frame
	more_lineups <- list(bat = data.table::fread(paste(path, "/MLB_Modeling/Betting/Predicted_Lineups/LotoQc_Batters_Clean.csv", sep = "")),
							pitch = data.table::fread(paste(path, "/MLB_Modeling/Betting/Predicted_Lineups/LotoQc_Pitchers_Clean.csv", sep = "")))



	################################################
	################################################


	################################################
	################ ADD TEAM STATS    #############
	################################################

	scores <- data$Scores

	dates <- sort(unique(scores$Date))
	teams <- sort(unique(scores$Team_Home))
	seasons <- sort(unique(scores$Season))

	n_dates <- length(dates)
	n_teams <- length(teams)

	processed_frames <- list()

	data.table::setkeyv(scores, c("Season"))
	n_iter <- length(seasons) * length(teams)
	print("Computing Team statistics ...", quote = FALSE)
	pb <- txtProgressBar(min = 0, max = n_iter, style = 3)
	k <- 1
	for(season in seasons){

		for(team in teams){

			matches_played <- scores[.(season)]
			matches_played <- matches_played[Team_Home == team | Team_Away == team]

			if(nrow(matches_played) == 0){next}

			away <- which(matches_played$Team_Away == team)

			matches_played[away, Win := 1 - matches_played[Team_Away == team, "Win"]] 
			matches_played[, Score := Score_Home]
			matches_played[away, Score := Score_Away]

			matches_played[, Score_vs := Score_Away]
			matches_played[away, Score_vs := Score_Home]

			matches_played <- matches_played[order(matches_played$Date), c("Date", "Score", "Score_vs", "Win")]
			matches_played[, Team := team]
			f <- function(x){cumsum(x) / c(1:length(x))}

			matches_played[, Win_p := f(Win)]
			matches_played[, PPG := f(Score)]
			matches_played[, PPG_vs := f(Score_vs)]
			matches_played[, PPG_diff := (PPG - PPG_vs)]

			f <- function(x){

				out <- rep(0, length(x))
				s <- 0

				for(i in 1:length(x)){

					if(s == 0){mult <- 0} else if(s > 0){mult <- x[i]} else if(s < 0){mult <- 1 - x[i]}
					s <- s * mult

					if(x[i] == 1){s <- s + 1} else{s <- s - 1}
					out[i] <- s

				}

				out <- c(0, out[-length(out)])

				return(out)

			}

			matches_played[, Streak := f(Win)]
			matches_played <- matches_played[, lapply(.SD, last), .SDcols = c(2:ncol(matches_played)), by = Date]

			matches_played[, Score_diff := (Score - Score_vs)]

			processed_frames[[length(processed_frames) + 1]] <- matches_played

			setTxtProgressBar(pb, k)
			k <- k + 1

		}

	}


	processed_frames <- dplyr::bind_rows(processed_frames)
	processed_frames <- processed_frames[order(Date, Team)]
	processed_frames <- data.table::as.data.table(processed_frames)

	rankings <- as.data.frame(matrix(nrow = n_dates * n_teams, ncol = 7))
	colnames(rankings) <- c("Date", "Team", "Win_p", "PPG", "PPG_vs", "PPG_diff", "Streak")
	rankings[1, ] <- processed_frames[1, colnames(rankings), with = FALSE]
	rankings <- data.table::as.data.table(rankings)

	srt <- c("Team", "Win_p", "PPG", "PPG_vs", "PPG_diff", "Streak")
	decr <- c("Win_p", "PPG", "PPG_diff", "Streak")
	incr <- c("PPG_vs")

	paste <- c("Win_p", "PPG", "PPG_vs", "PPG_diff", "Streak")

	n_iter <- n_dates 
	print("Computing Team rankings ...", quote = FALSE)
	pb <- txtProgressBar(min = 0, max = n_iter, style = 3)
	k <- 1
	for(i in 1:n_dates){

		subframe <- processed_frames[Date <= dates[i]]
		subframe <- subframe[, lapply(.SD, last), .SDcols = srt, by = Team]

		data.table::setDT(subframe)[, (decr) := lapply(.SD, order, decreasing = TRUE), .SDcols = decr]
		data.table::setDT(subframe)[, (incr) := lapply(.SD, order, decreasing = FALSE), .SDcols = incr]

		data.table::setorder(subframe, Team)

		frm <- (i-1)*n_teams + 1
		to <- i*n_teams
		index <- c(frm:to)[match(subframe$Team, teams)]
		fill <- c(frm:to)[-match(subframe$Team, teams)]

		data.table::set(rankings, i = c(frm:to), j = match("Team", colnames(rankings)), value = teams)
		data.table::set(rankings, i = c(frm:to), j = match("Date", colnames(rankings)), value = dates[i])

		data.table::set(rankings, i = index, j = match(paste, colnames(rankings)), value = subframe[, paste, with = FALSE])

		if(length(fill) > 0){

			data.table::set(rankings, i = fill, j = match(paste, colnames(rankings)), value = 0)

		}

		setTxtProgressBar(pb, k)
		k <- k + 1

	}
	rankings[, Date := as.Date(Date, origin = "1970-01-01")]




	################################################
	################################################


	################################################
	################ SPLIT DATA     ################
	################################################

	splitted_data <- list()

	seasons <- sort(unlist(unique(data$Scores[, (Season)])))
	teams <- sort(unlist(unique(data$Scores[, (Team_Home)])))


	for(i in 2:length(data)){

		data.table::setkeyv(data[[i]], c("Season", "Team"))

	}

	processed_frames[, Season := as.numeric(stringr::str_split(processed_frames$Date, "-", simplify = TRUE)[, 1])]
	rankings[, Season := as.numeric(stringr::str_split(rankings$Date, "-", simplify = TRUE)[, 1])]

	data.table::setkeyv(processed_frames, c("Season", "Team"))
	data.table::setkeyv(rankings, c("Season", "Team"))

	for(season in seasons){

		splitted_data[[length(splitted_data) + 1]] <- list()
		print(paste("Querying", season, "season..."), quote = FALSE)

		for(team in teams){

			print(paste("Querying", team, "..."), quote = FALSE)
			#Query according to seasons and teams
			k <-length(splitted_data)


			splitted_data[[k]][[length(splitted_data[[k]]) + 1]] <- list()

			splitted_data[[k]][[length(splitted_data[[k]])]]$Bat <- data$Bat[.(season, team)]
			splitted_data[[k]][[length(splitted_data[[k]])]]$Pitch <- data$Pitch[.(season, team)]

			splitted_data[[k]][[length(splitted_data[[k]])]]$Bat_Lineups <- data$Bat_Lineups[.(season, team)]
			splitted_data[[k]][[length(splitted_data[[k]])]]$Pitch_Lineups <- data$Pitch_Lineups[.(season, team)]

			splitted_data[[k]][[length(splitted_data[[k]])]]$Bat_Lineups <- data$Bat_Lineups[.(season, team)]
			splitted_data[[k]][[length(splitted_data[[k]])]]$Pitch_Lineups <- data$Pitch_Lineups[.(season, team)]	
			
			splitted_data[[k]][[length(splitted_data[[k]])]]$Team_Stats <- processed_frames[.(season, team)]
			splitted_data[[k]][[length(splitted_data[[k]])]]$Team_Rankings <- rankings[.(season, team)]	

			#Add weights
			weight_colname <- c("PA", "IP")
			for(i in 1:2){

				weights <- splitted_data[[k]][[length(splitted_data[[k]])]][[i]][, c("ID", weight_colname[i]), with = FALSE]
				weights <- weights[, lapply(.SD, function(x){return(x / sum(x))}), by = ID, .SDcols = weight_colname[i]]

				splitted_data[[k]][[length(splitted_data[[k]])]][[i]][, Weight := weights[, weight_colname[i], with = FALSE][[1]]]

			}

		}

		names(splitted_data[[length(splitted_data)]]) <- teams

		print("", quote = FALSE)
		print("###################################", quote = FALSE)
		print("", quote = FALSE)

	}

	names(splitted_data) <- seasons


	################################################
	################################################



	################################################
	######### ANNOUNCED / RANDOM SPLIT   ###########
	################################################

	#Loop over seasons
	n_annc <- list(f = function(x){7 <= length(which(x == "Yes"))},
					g = function(x){1 == length(which(x == "Yes"))})

	col_names <- c("Bat_Home_Av", "Pitch_Home_Av", "Bat_Away_Av", "Pitch_Away_Av")
	scores[, (col_names) := FALSE]

	data.table::setkey(scores, "ID")


	for(i in 1:length(splitted_data)){

		print(paste("Tagging stating lineups...", seasons[i], "season..."), quote = FALSE)
		pb <- txtProgressBar(min = 0, max = length(splitted_data[[i]]), style = 3)

		#Loop over teams
		for(j in 1:length(splitted_data[[i]])){

			#Loop over bat, pitch
			for(k in 1:2){

				#Tag announced batters and pitchers
				to_join <- splitted_data[[i]][[j]][[k + 2]][, c("ID", "Name")]
				to_join[, Announced := "Yes"]

				data.table::setkeyv(splitted_data[[i]][[j]][[k]], c("Name", "ID"))
				data.table::setkeyv(to_join, c("Name", "ID"))

				splitted_data[[i]][[j]][[k]][to_join, Announced := i.Announced]

				#Check if lineups are missing
				annc_per <- splitted_data[[i]][[j]][[k]][, lapply(.SD, n_annc[[k]]), .SDcols = c("Announced"), by = c("ID")]
				loc <- splitted_data[[i]][[j]][[k]][, lapply(.SD, last), .SDcols = c("Location"), by = c("ID")]
				data.table::setkey(annc_per, "ID")
				data.table::setkey(loc, "ID")

				#Fill the score frame (tag avaible lineups -- debugging purpose)
				annc_per[loc, Location := i.Location]

				home <- annc_per[Location == "Home"]
				away <- annc_per[Location == "Away"]

				if(k == 1){

					scores[annc_per[Location == "Home"], Bat_Home_Av := i.Announced]
					scores[annc_per[Location == "Away"], Bat_Away_Av := i.Announced]

				} else {

					scores[annc_per[Location == "Home"], Pitch_Home_Av := i.Announced]
					scores[annc_per[Location == "Away"], Pitch_Away_Av := i.Announced]

				}
				

			}

			setTxtProgressBar(pb, j)

		}

		print("", quote = FALSE)
		print("###################################", quote = FALSE)
		print("", quote = FALSE)

	}

	scores[, Avaible_for_Regr := apply(scores[, c("Bat_Home_Av", "Pitch_Home_Av", "Bat_Away_Av", "Pitch_Away_Av")], 1, function(x){any(!x) == FALSE})]
	avaible_p <- round(100 * length(which(scores$Avaible_for_Regr)) / nrow(scores), 2)

	print(paste(avaible_p, "% of the scrapped data is avaible for regression purposes.", sep = ""), quote = FALSE)



	for(i in 1:length(splitted_data)){

		for(j in 1:length(splitted_data[[i]])){

			splitted_data[[i]][[j]]$Bat_Lineups <- NULL
			splitted_data[[i]][[j]]$Pitch_Lineups <- NULL

			#Nth match played dummy function
			g <- function(x){
		
				unique_vals <- sort(unique(x))
				return(match(x, unique_vals))
			}

			for(k in 1:4){

				data.table::setorder(splitted_data[[i]][[j]][[k]], Date)
				splitted_data[[i]][[j]][[k]][, Nth_match := g(Date)]

			}

			data.table::setkeyv(splitted_data[[i]][[j]]$Bat, c("Announced"))
			splitted_data[[i]][[j]]$Bat_Relief <- splitted_data[[i]][[j]]$Bat[.("No")]

			data.table::setkeyv(splitted_data[[i]][[j]]$Pitch, c("Announced"))
			splitted_data[[i]][[j]]$Pitch_Relief <- splitted_data[[i]][[j]]$Pitch[.("No")]

			#Re-order
			splitted_data[[i]][[j]] <- splitted_data[[i]][[j]][c(1,2,5,6,3,4)]


			#Nth match played dummy function
			Geom_MLE <- function(x){
		
				d <- diff(c(0, x)) 
				d <- c(1:length(d)) / cumsum(d)
				return(d)

			}		

			for(k in 1:4){

				splitted_data[[i]][[j]][[k]][, Geom_MLE := Geom_MLE(Nth_match), by = Name]

			}

		}

	}

	################################################
	################################################


	################################################
	########### ADD MONEYLINES           ###########
	################################################

	print("Adding moneyline data ...", quote = FALSE)

	ID <- data$Scores$ID

	#Retrieve moneyline frame 
	#Historical
	ML <- data.table::fread(paste(path, "/MLB_Modeling/Betting/Clean_Data/MLB_Odds.csv", sep = ""))
	ML <- ML[ID != -1] %>%
			.[, (c("Pitcher_Home", "Pitcher_Away")) := NULL]



	data.table::setkey(ML, "ID")

	#Moneylines
	scores[ML, Factor_Home_Historical := round(i.Close_Home + 1, 2)]
	scores[ML, Factor_Away_Historical := round(i.Close_Away + 1, 2)]

	#Over / Under
	scores[ML, OU_Home_Historical := i.CloseOU_Home]
	scores[ML, OU_Away_Historical := i.CloseOU_Away]

	scores[ML, OU_ML_Home_Historical := round(i.OU_ML_Close_Home + 1, 2)]
	scores[ML, OU_ML_Away_Historical := round(i.OU_ML_Close_Away + 1, 2)]

	#RunLine
	scores[ML, RunLine_Home_Historical := i.RunLine_Home]
	scores[ML, RunLine_Away_Historical := i.RunLine_Away]

	scores[ML, RunLine_ML_Home_Historical := round(i.RunLine_ML_Home + 1, 2)]
	scores[ML, RunLine_ML_Away_Historical := round(i.RunLine_ML_Away + 1, 2)]


	#Find the profit margin. 
	#Assummed to be the same on both sides, i.e.: 1 / (1 + r_1 + c) + 1/(1 + r_2 + c) = 1

	b <- scores$Factor_Home_Historical + scores$Factor_Away_Historical - 2
	c <- scores$Factor_Home_Historical * scores$Factor_Away_Historical - scores$Factor_Home_Historical - scores$Factor_Away_Historical

	scores[, Profit_Margin_Historical := (-b + sqrt(b^2 - 4*c)) / 2]
	scores[, P_Home_Win_Historical := 1 / (Factor_Home_Historical + Profit_Margin_Historical)]

	#LotoQuebec data
	ML_LotoQc <- data.table::fread(paste(path, "/MLB_Modeling/Betting/Predicted_Lineups/LotoQc_Moneylines_Clean.csv", sep = ""))
	data.table::setkeyv(scores, c("Date", "Team_Home", "Team_Away"))
	data.table::setkeyv(ML_LotoQc, c("Date", "Team_Home", "Team_Away"))

	scores[ML_LotoQc, Factor_Home_LotoQc := round(i.Factor_Home, 2)]
	scores[ML_LotoQc, Factor_Away_LotoQc := round(i.Factor_Away, 2)]

	b <- scores$Factor_Home_LotoQc + scores$Factor_Away_LotoQc - 2
	c <- scores$Factor_Home_LotoQc * scores$Factor_Away_LotoQc - scores$Factor_Home_LotoQc - scores$Factor_Away_LotoQc

	scores[, Profit_Margin_LotoQc := (-b + sqrt(b^2 - 4*c)) / 2]
	scores[, P_Home_Win_LotoQc := 1 / (Factor_Home_LotoQc + Profit_Margin_LotoQc)]


	#Approximation for missing LotoQc data
	mean_LotoQc_profit_margin <- mean(na.omit(scores$Profit_Margin_LotoQc))

	scores[, Factor_Home_LotoQc_Approx := Factor_Home_LotoQc]
	scores[, Factor_Away_LotoQc_Approx := Factor_Away_LotoQc]
	scores[, Profit_Margin_LotoQc_Approx := Profit_Margin_LotoQc]

	missing_val_index_LotoQc <- which(is.na(scores$Factor_Home_LotoQc) | is.na(scores$Factor_Away_LotoQc))
	scores[missing_val_index_LotoQc, Factor_Home_LotoQc_Approx := Factor_Home_Historical + Profit_Margin_Historical - mean_LotoQc_profit_margin]
	scores[missing_val_index_LotoQc, Factor_Away_LotoQc_Approx := Factor_Away_Historical + Profit_Margin_Historical - mean_LotoQc_profit_margin]
	scores[missing_val_index_LotoQc, Profit_Margin_LotoQc_Approx := mean_LotoQc_profit_margin]

	scores[, P_Home_Win_LotoQc_Approx := 1 / (Factor_Away_LotoQc_Approx + Profit_Margin_LotoQc_Approx)]

	#Tag matches with avaible moneylines data
	scores[, Avaible_for_Betting := scores$Avaible_for_Regr & !is.na(scores$P_Home_Win_LotoQc_Approx)]
	avaible_p <- round(100 * length(which(scores$Avaible_for_Betting)) / nrow(scores), 2)

	print(paste(avaible_p, "% of the scrapped data is avaible for betting strategy testing purposes.", sep = ""), quote = FALSE)



	################################################
	################################################


	for(i in 1:length(splitted_data)){

		for(j in 1:length(splitted_data[[i]])){

			for(k in 1:length(splitted_data[[i]][[j]])){

				splitted_data[[i]][[j]][[k]]

				out <- grepl("Unnamed", names(splitted_data[[i]][[j]][[k]]))
				if(any(out)){

					out <- names(splitted_data[[i]][[j]][[k]])[which(out)]
					splitted_data[[i]][[j]][[k]][, (out) := NULL]

				}

			}

		}

	}



	################################################
	############## SAVE DATABASE    ################
	################################################

	print("Done. Saving ...", quote = FALSE)

	save_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds", sep = "")
	save_path_2 <- paste(path, "/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds", sep = "")

	saveRDS(splitted_data, save_path)
	saveRDS(scores, save_path_2)

	################################################
	################################################
		
}




