#' Initiate or update the bets database
#'
#' This function creates the database with the bets offered by Pinnacle (half innings and game, select bets)
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


process_Pinnacle_data <- function(path){

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
	path_check <- paste(folder_directory, "/Betting_Database_Pinnacle.rds", sep = "")
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
		file_path <- paste(f_path, "/Bets_Pinnacle.csv", sep = "")


		if(date %in% dates_done | !file.exists(file_path)){

			next

		}

		bets[[length(bets) + 1]] <- data.table::fread(file_path)

		bets[[length(bets)]][, Scrapping_Time := as.POSIXct(Scrapping_Time)]
		bets[[length(bets)]][, Game_Time := as.POSIXct(Game_Time)]

		bets[[length(bets)]] <- bets[[length(bets)]][Minutes_Until_Start <= 12 * 60]

		#Retain the betting opportunities avaible before the first match started
		#I.e.: we can use portfolio theory to allocate different capital sums to different betting opportunities
		#This wouldn't be possible if we used information given to us AFTER the first match started
		max_time <- min(bets[[length(bets)]]$Game_Time)
		bets[[length(bets)]] <- bets[[length(bets)]][Scrapping_Time <= max_time]		

		#Retain the most recently scrapped values for each pair of teams
		bets[[length(bets)]][, Most_Recent := lapply(.SD, function(x){x == max(x)}),
								.SDcols = "Scrapping_Time"]

		bets[[length(bets)]] <- bets[[length(bets)]][Most_Recent == TRUE]

		bets[[length(bets)]][, Most_Recent := NULL]


		#Fix an error where the half-matches were tagged as Inn. = 9 instead of Inn. = 5
		if(date <= "2021-07-6"){

			combinations <- unique(bets[[length(bets)]][, c("Team_Home", "Team_Away", "Bet_Type")])
			combinations[, Bet_ID := c(1:nrow(combinations))]

			for(i in 1:nrow(combinations)){

				index <- bets[[length(bets)]][Team_Home == combinations$Team_Home[i] &
												Team_Away == combinations$Team_Away[i] &
												Bet_Type == combinations$Bet_Type[i], which = TRUE]

				gap <- which(diff(index) != 1) 
				if(!any(gap)){next}

				gap <- gap[1] + 1
				index <- index[gap:length(index)]

				bets[[length(bets)]][index, Inn. := 5]

			}


		}		


	}

	if(length(bets) == 0){

		print("No new matches to process.", quote = FALSE)
		return(NULL)

	}

	names(bets) <- c(1:length(bets))
	bets <- dplyr::bind_rows(bets)

	#################################################################################################
	#################################################################################################

	print("Saving...", quote = FALSE)
	#Save if no previous file was found
	if(!update){

		saveRDS(bets, paste(folder_directory, "/Betting_Database_Pinnacle.rds", sep = ""))

	#Else update
	} else {

		temp <- data.table::copy(unique(rbind(temp, bets)))

		#temp[, Most_Recent := lapply(.SD, function(x){x == min(x)}),
								#by = c("Team_Home", "Team_Away", "Date"),
								#.SDcols = "Minutes_Until_Start"]

		#temp <- temp[Most_Recent == TRUE]
		#temp[, Most_Recent := NULL]

		saveRDS(temp, paste(folder_directory, "/Betting_Database_Pinnacle.rds", sep = ""))

	}

	print("Done.", quote = FALSE)

}