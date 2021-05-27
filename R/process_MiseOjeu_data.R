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


		#Retain the most recently scrapped values for each pair of teams
		combinations <- unique(bets[[length(bets)]][, c("Team_Home", "Team_Away", "Scrapping_Time"), with = FALSE])
		combinations <- combinations[Team_Home != "" & Team_Away != ""]

		combinations <- combinations[, lapply(.SD, max), by = c("Team_Home", "Team_Away"), .SDcols = "Scrapping_Time"]

		data.table::setkeyv(bets[[length(bets)]], c("Team_Home", "Team_Away", "Scrapping_Time"))
		data.table::setkeyv(combinations, c("Team_Home", "Team_Away", "Scrapping_Time"))

		bets[[length(bets)]] <- bets[[length(bets)]][combinations]
		bets[[length(bets)]][, Game_Starts_In := NULL]
		bets[[length(bets)]][, Game_Started := NULL]


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


	#-------------------------------------------------
	#-------------------------------------------------
	bets_DB$"PREMIER ARRIVE" <- list(PREMIER = bets_by_cat$`PREMIER ARRIVE`[Bet_On2 != "None"])


	print("Saving...", quote = FALSE)
	#Save if no previous file was found
	if(!update){

		saveRDS(bets_DB, paste(folder_directory, "/Betting_Database.rds", sep = ""))

	#Else update
	} else {

		for(i in 1:length(bets_DB)){

			a <- names(bets_DB[[i]][[j]])
			b <- names(temp[[i]][[j]])

			indices <- match(a, b)

			for(j in 1:length(indices)){

				temp[[i]][[indices[j]]] <- rbind(temp[[i]][[indices[j]]][Date < today], bets_DB[[i]][[j]])

			}

		}

		saveRDS(temp, paste(folder_directory, "/Betting_Database.rds", sep = ""))


	}

	print("Done.", quote = FALSE)

}


