#' Initiate or update the bets database
#'
#' This function creates the database with the bets offered by Loto-Quebec
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


process_MiseOjeu_data <- function(path){

	#################################################################################################
	#################################################################################################
	#########      Extract the files
	#################################################################################################
	#################################################################################################


	folder_directory <- paste(path, "/MLB_Modeling/Betting/Predicted_Lineups", sep = "")
	folders <- list.dirs(path = folder_directory, full.names = TRUE, recursive = TRUE)[-1]

	if(length(folders) == 0){

		print("Error: no bet webscrapped yet.", quote = FALSE)
		print(paste("Missing files should be located at:", folder_directory), quote = FALSE)
		return(NULL)

	}


	#Check if some matches have already been processed
	update <- FALSE
	dates_done <- c()	
	path_check <- paste(folder_directory, "/Betting_Database.rds", sep = "")
	if(file.exists(path_check)){

		temp <- readRDS(path_check)
		dates_done <- unique(temp$GAGNANT$moneyline$Date)

		today <- Sys.Date()
		dates_done <- dates_done[dates_done < today]

		update <- TRUE


	}


	bets <- list()
	print("Extracting data...", quote = FALSE)
	for(f_path in folders){

		#Skip if already processed 
		date <- as.Date(rev(stringr::str_split(f_path, "/")[[1]])[1], "%d-%m-%Y")
		if(date %in% dates_done){

			next

		}

		bets[[length(bets) + 1]] <- data.table::fread(paste(f_path, "/Bets.csv", sep = ""))

		#Remove the games where the match has already started
		bets[[length(bets)]] <- bets[[length(bets)]][Game_Started == FALSE]

		bets[[length(bets)]][, Scrapping_Time := as.POSIXct(Scrapping_Time)]
		bets[[length(bets)]][, Game_Time := as.POSIXct(Game_Time)]
		bets[[length(bets)]][, Date := as.Date(stringr::str_split(Game_Time, " ", simplify = TRUE)[, 1])]


		bets[[length(bets)]][, Minutes_Until_Start := (Game_Time - Scrapping_Time)/60]

		#Remove games taking place tomorrow rather than today
		bets[[length(bets)]] <- bets[[length(bets)]][Minutes_Until_Start <= 12 * 60]

		#Retain the betting opportunities avaible before the first match started
		#I.e.: we can use portfolio theory to allocate different capital sums to different betting opportunities
		#This wouldn't be possible if we used information given to us AFTER the first match started
		max_time <- min(bets[[length(bets)]]$Game_Time)
		bets[[length(bets)]] <- bets[[length(bets)]][Scrapping_Time <= max_time]

		#Retain the most recently scrapped values for each pair of teams
		bets[[length(bets)]][, Most_Recent := lapply(.SD, function(x){x == min(x)}),
								by = c("Team_Home", "Team_Away"),
								.SDcols = "Minutes_Until_Start"]

		bets[[length(bets)]] <- bets[[length(bets)]][Most_Recent == TRUE]

		bets[[length(bets)]][, Most_Recent := NULL]
		bets[[length(bets)]][, Game_Starts_In := NULL]
		bets[[length(bets)]][, Game_Started := NULL]


	}

	if(length(bets) == 0){

		print("No new matches to process.", quote = FALSE)
		return(NULL)

	}

	names(bets) <- c(1:length(bets))
	bets <- dplyr::bind_rows(bets)

	#################################################################################################
	#################################################################################################



	#################################################################################################
	#################################################################################################
	#########      Data cleaning
	#################################################################################################
	#################################################################################################

	bets_DB <- list()

	#Remove bets on individual players
	bets <- bets[which(!(grepl("TOTAL DE COUPS SURS", bets$Bet_Type)))]
	bets <- bets[which(!(grepl("AU BATON", bets$Bet_Type)))]
	bets <- bets[which(!(grepl("CIRCUITS", bets$Bet_Type)))]

	#Divide by bet categories
	categories <- c("GAGNANT", "ECART", "TOTAL", "MARGE", "PREMIER ARRIVE")

	bets_by_cat <- list()
	for(cat in categories){

		bets_by_cat[[length(bets_by_cat) + 1]] <- bets[grepl(cat, Bet_Type, fixed = TRUE), names(bets), with = FALSE]

	}
	names(bets_by_cat) <- categories


	#-------------------------------------------------
	#-------------------------------------------------
	#Process bets on simple wins
	bets_DB$GAGNANT <- list()

	#Not in OT
	not_overtime <- which(grepl("rÃ©glementaire", bets_by_cat$GAGNANT$Bet_On))
	bets_DB$GAGNANT$not_overtime <- bets_by_cat$GAGNANT[not_overtime, names(bets_by_cat$GAGNANT), with = FALSE]
	bets_by_cat$GAGNANT <- bets_by_cat$GAGNANT[-not_overtime]

	#In OT
	overtime <- which(grepl("supplÃ©mentaires", bets_by_cat$GAGNANT$Bet_On))
	bets_DB$GAGNANT$overtime <- bets_by_cat$GAGNANT[overtime, names(bets_by_cat$GAGNANT), with = FALSE]
	bets_by_cat$GAGNANT <- bets_by_cat$GAGNANT[-overtime]

	#On the match, OT or not
	on_win <- bets_by_cat$GAGNANT[Bet_Type == "GAGNANT A 2 ISSUES", which = TRUE]
	bets_DB$GAGNANT$moneyline <- bets_by_cat$GAGNANT[on_win, names(bets_by_cat$GAGNANT), with = FALSE]
	bets_by_cat$GAGNANT <- bets_by_cat$GAGNANT[-on_win]

	#Rest
	remaining <- sort(unique(bets_by_cat$GAGNANT$Bet_Type))
	for(j in remaining){

		bets_DB$GAGNANT[[length(bets_DB$GAGNANT) + 1]] <- bets_by_cat$GAGNANT[Bet_Type == j, names(bets_by_cat$GAGNANT), with = FALSE]
		names(bets_DB$GAGNANT)[length(bets_DB$GAGNANT)] <- j

	}


	for(i in 1:length(bets_DB$GAGNANT)){

		bets_DB$GAGNANT[[i]][Bet_On2 == "None" & Team_Home == "PHI", Bet_On2 := "Home"]
		bets_DB$GAGNANT[[i]][Bet_On2 == "None" & Team_Away == "PHI", Bet_On2 := "Away"]

	}


	#-------------------------------------------------
	#-------------------------------------------------
	bets_DB$ECART <- list()

	#Spreads on specific innings
	string_name <- "PREMIERES MANCHES"
	n_innings <- c(3, 5, 7)

	for(n in n_innings){

		target <- paste(n, string_name)
		index <- which(grepl(target, bets_by_cat$ECART$Bet_Type))

		bets_DB$ECART[[length(bets_DB$ECART) + 1]] <- bets_by_cat$ECART[index, names(bets_by_cat$ECART), with = FALSE]
		bets_by_cat$ECART <- bets_by_cat$ECART[-index]

	}
	names(bets_DB$ECART) <- paste(n_innings, string_name)

	bets_DB$ECART$MATCH <- bets_by_cat$ECART[, names(bets_by_cat$ECART), with = FALSE]


	for(i in 1:length(bets_DB$ECART)){

		bets_DB$ECART[[i]][Bet_On2 == "None" & Team_Home == "PHI", Bet_On2 := "Home"]
		bets_DB$ECART[[i]][Bet_On2 == "None" & Team_Away == "PHI", Bet_On2 := "Away"]

	}


	#-------------------------------------------------
	#-------------------------------------------------
	bets_DB$TOTAL <- list()


	#Odd/even
	index <- which(grepl("PAIR/IMPAIR", bets_by_cat$TOTAL$Bet_Type))
	rmv <- c()
	#After n innings
	string_name <- "PREMIERES MANCHES"
	n_innings <- c(3, 5, 7)

	for(n in n_innings){

		target <- paste(n, string_name)
		index2 <- which(grepl(target, bets_by_cat$TOTAL$Bet_Type[index]))

		bets_DB$TOTAL[[length(bets_DB$TOTAL) + 1]] <- bets_by_cat$TOTAL[index[index2], names(bets_by_cat$TOTAL), with = FALSE]
		rmv <- c(rmv, index2)

		names(bets_DB$TOTAL)[length(bets_DB$TOTAL)] <- paste("PAIR/IMPAIR", target)

	}

	bets_DB$TOTAL$"PAIR/IMPAIR MATCH" <- bets_by_cat$TOTAL[index[-rmv], names(bets_by_cat$TOTAL), with = FALSE]
	bets_by_cat$TOTAL <- bets_by_cat$TOTAL[-index]




	index <- bets_by_cat$TOTAL[Bet_Type == "1ERE MANCHE - TOTAL DE POINTS", which = TRUE]
	bets_DB$TOTAL$"1ERE MANCHE" <- bets_by_cat$TOTAL[index, names(bets_by_cat$TOTAL), with = FALSE]
	bets_by_cat$TOTAL <- bets_by_cat$TOTAL[-index]


	index <- which(grepl("PLUS/MOINS", bets_by_cat$TOTAL$Bet_Type))
	rmv <- c()
	#After n innings
	string_name <- "MANCHES"
	n_innings <- c(3, 5, 7)

	for(n in n_innings){

		target <- paste(n, string_name)
		index2 <- which(grepl(target, bets_by_cat$TOTAL$Bet_Type[index]))

		bets_DB$TOTAL[[length(bets_DB$TOTAL) + 1]] <- bets_by_cat$TOTAL[index[index2], names(bets_by_cat$TOTAL), with = FALSE]
		rmv <- c(rmv, index2)

		names(bets_DB$TOTAL)[length(bets_DB$TOTAL)] <- paste("BOTH TEAM", target)

	}

	bets_DB$TOTAL$"BOTH TEAM MATCH" <- bets_by_cat$TOTAL[index[-rmv], names(bets_by_cat$TOTAL), with = FALSE]
	bets_by_cat$TOTAL <- bets_by_cat$TOTAL[-index]



	#Individual teams
	string_name <- "PREMIERES MANCHES"
	n_innings <- c(3, 5, 7)

	for(n in n_innings){

		target <- paste(n, string_name)
		index <- which(grepl(target, bets_by_cat$TOTAL$Bet_Type))

		bets_DB$TOTAL[[length(bets_DB$TOTAL) + 1]] <- bets_by_cat$TOTAL[index, names(bets_by_cat$TOTAL), with = FALSE]
		bets_by_cat$TOTAL <- bets_by_cat$TOTAL[-index]

		names(bets_DB$TOTAL)[length(bets_DB$TOTAL)] <- paste("SINGLE TEAM", target)

	}

	bets_DB$TOTAL$"SINGLE TEAM MATCH" <- bets_by_cat$TOTAL[, names(bets_by_cat$TOTAL), with = FALSE]


	for(i in 1:length(bets_DB$TOTAL)){

		if(grepl("SINGLE", names(bets_DB$TOTAL)[i], fixed = TRUE)){

			bets_DB$TOTAL[[i]][Bet_On2 == "None" & Team_Home == "PHI", Bet_On2 := "Home"]
			bets_DB$TOTAL[[i]][Bet_On2 == "None" & Team_Away == "PHI", Bet_On2 := "Away"]

		}


	}



	#-------------------------------------------------
	#-------------------------------------------------
	bets_DB$MARGE <- list()

	#Individual teams
	string_name <- "PREMIERES MANCHES"
	n_innings <- c(3, 5, 7)

	for(n in n_innings){

		target <- paste(n, string_name)
		index <- which(grepl(target, bets_by_cat$MARGE$Bet_Type))

		bets_DB$MARGE[[length(bets_DB$MARGE) + 1]] <- bets_by_cat$MARGE[index, names(bets_by_cat$MARGE), with = FALSE]
		bets_by_cat$MARGE <- bets_by_cat$MARGE[-index]

		names(bets_DB$MARGE)[length(bets_DB$MARGE)] <- target

	}

	bets_DB$MARGE$MATCH <- bets_by_cat$MARGE[, names(bets_by_cat$MARGE), with = FALSE]

	for(i in 1:length(bets_DB$MARGE)){

		bets_DB$MARGE[[i]][Bet_On2 == "None" & Team_Home == "PHI" & Bet_On != "Nul", Bet_On2 := "Home"]
		bets_DB$MARGE[[i]][Bet_On2 == "None" & Team_Away == "PHI" & Bet_On != "Nul", Bet_On2 := "Away"]

	}



	#-------------------------------------------------
	#-------------------------------------------------
	bets_DB$"PREMIER ARRIVE" <- list(PREMIER = bets_by_cat$`PREMIER ARRIVE`)
	bets_DB$"PREMIER ARRIVE"$PREMIER[Bet_On2 == "None" & Team_Home == "PHI" & Bet_On != "Aucun e", Bet_On2 := "Home"]
	bets_DB$"PREMIER ARRIVE"$PREMIER[Bet_On2 == "None" & Team_Home == "PHI" & Bet_On != "Aucun e", Bet_On2 := "Away"]


	#-------------------------------------------------
	#-------------------------------------------------
	#Combine everything into one frame

	#-------------------------------------------------
	#GAGNANT
	innings <- c(100, 99, 9, 1, 3, 5, 7)
	for(j in 1:length(innings)){

		bets_DB$GAGNANT[[j]][, Inn. := eval(innings[j])]
		bets_DB$GAGNANT[[j]][, Bet_Type := "WINNER"]

		bets_DB$GAGNANT[[j]][Bet_On2 == "Home", Bet_On := Team_Home]
		bets_DB$GAGNANT[[j]][Bet_On2 == "Away", Bet_On := Team_Away]
		bets_DB$GAGNANT[[j]][Bet_On2 == "None", Bet_On := "None"]

	}


	#ECART
	innings <- seq(from = 3, to = 9, by = 2)
	for(j in 1:length(innings)){

		bets_DB$ECART[[j]][, Inn. := eval(innings[j])]
		bets_DB$ECART[[j]][, Bet_Type := "SPREAD"]

		bets_DB$ECART[[j]][Bet_On2 == "Home", Bet_On := Team_Home]
		bets_DB$ECART[[j]][Bet_On2 == "Away", Bet_On := Team_Away]
		bets_DB$ECART[[j]][Bet_On2 == "None", Bet_On := "None"]

	}


	#TOTAL, ODD/EVEN
	nm <- names(bets_DB$TOTAL)
	indices <- list(odd_even = which(grepl("PAIR", nm, fixed = TRUE)),
						both = which(!grepl("PAIR", nm, fixed = TRUE) & !grepl("SINGLE", nm, fixed = TRUE)), 
						single = which(grepl("SINGLE", nm, fixed = TRUE)))

	innings <- list(odd_even = seq(from = 3, to = 9, by = 2),
							both = seq(from = 1, to = 9, by = 2),
							single = seq(from = 3, to = 9, by = 2))

	bet_type <- c("ODD/EVEN", "SUM", "POINTS")

	for(k in 1:length(indices)){

		for(j in 1:length(indices[[k]])){

			bets_DB$TOTAL[[indices[[k]][j]]][, Inn. := eval(innings[[k]][j])]
			bets_DB$TOTAL[[indices[[k]][j]]][, Bet_Type := bet_type[k]]

			if(k > 1){

				x <- which(grepl("Plus", bets_DB$TOTAL[[indices[[k]][j]]]$Bet_On, fixed = TRUE))
				bets_DB$TOTAL[[indices[[k]][j]]][x, Bet_On := "Above"]
				bets_DB$TOTAL[[indices[[k]][j]]][-x, Bet_On := "Below"]

			} else {

				bets_DB$TOTAL[[indices[[k]][j]]][Bet_On == "Impair", Bet_On := "Odd"]
				bets_DB$TOTAL[[indices[[k]][j]]][Bet_On == "Pair", Bet_On := "Even"]

			}

		}

	}

	#MARGIN
	innings <- seq(from = 3, to = 9, by = 2)
	for(j in 1:length(innings)){

		bets_DB$MARGE[[j]] <- bets_DB$MARGE[[j]][Bet_On2 != "None"]
		bets_DB$MARGE[[j]][Bet_Spread == 0, Bet_Spread := 6]

		bets_DB$MARGE[[j]][, Inn. := eval(innings[j])]
		bets_DB$MARGE[[j]][, Bet_Type := "MARGIN"]

		bets_DB$MARGE[[j]][Bet_On2 == "Home", Bet_On := Team_Home]
		bets_DB$MARGE[[j]][Bet_On2 == "Away", Bet_On := Team_Away]
		bets_DB$MARGE[[j]][Bet_On2 == "None", Bet_On := "None"]

	}

	#1st
	bets_DB$`PREMIER ARRIVE`$PREMIER[, Inn. := 9]
	bets_DB$`PREMIER ARRIVE`$PREMIER[, Bet_Spread := as.integer(stringr::str_split(bets_DB$`PREMIER ARRIVE`$PREMIER$Bet_Type, " ", simplify = TRUE)[, 4])]

	bets_DB$`PREMIER ARRIVE`$PREMIER[, Bet_Type := "FIRST"]

	bets_DB$`PREMIER ARRIVE`$PREMIER[Bet_On2 == "Home", Bet_On := Team_Home]
	bets_DB$`PREMIER ARRIVE`$PREMIER[Bet_On2 == "Away", Bet_On := Team_Away]
	bets_DB$`PREMIER ARRIVE`$PREMIER[Bet_On2 == "None", Bet_On := "None"]

	bets_DB <- lapply(bets_DB, function(x){data.table::copy(dplyr::bind_rows(x))})
	bets_DB <- data.table::copy(dplyr::bind_rows(bets_DB))

	names(bets_DB)[which(names(bets_DB) == "Bet_Spread")] <- "Bet_Type2"


	print("Saving...", quote = FALSE)
	#Save if no previous file was found
	if(!update){

		saveRDS(bets_DB, paste(folder_directory, "/Betting_Database.rds", sep = ""))

	#Else update
	} else {

		temp <- data.table::copy(unique(rbind(temp, bets_DB)))

		temp[, Most_Recent := lapply(.SD, function(x){x == min(x)}),
								by = c("Team_Home", "Team_Away", "Date"),
								.SDcols = "Minutes_Until_Start"]

		temp <- temp[Most_Recent == TRUE]
		temp[, Most_Recent := NULL]

		saveRDS(temp, paste(folder_directory, "/Betting_Database.rds", sep = ""))

	}

	print("Done.", quote = FALSE)

}


