#' Adds the results of each bets.
#'
#' This function loops over the avaible data and adds the outcomes of each avaible bet.
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


Add_Realizations <- function(path){

	folder_directory <- paste(path, "/MLB_Modeling/Betting/Predicted_Lineups", sep = "")
	folders <- list.dirs(path = folder_directory, full.names = TRUE, recursive = TRUE)[-1]
	if(length(folders) == 0){

		print("Error: no bet webscrapped yet.", quote = FALSE)
		print(paste("Missing files should be located at:", folder_directory), quote = FALSE)
		return(NULL)

	}


	path_check <- paste(path, "/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("File missing at:", path_check, sep = ""), quote = FALSE)

	}
	scores <- readRDS(path_check)


	path_check <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Markov_Database.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("File missing at:", path_check, sep = ""), quote = FALSE)

	}
	play_by_play <- readRDS(path_check)

	states <- data.table::copy(play_by_play$states)
	play_by_play$states <- NULL


	seasons <- unique(sapply(stringr::str_split(folders, "/", simplify = TRUE)[, 6], function(x){stringr::str_split(x, "-")[[1]][3]}))
	play_by_play <- play_by_play[which(names(play_by_play) %in% seasons)]
	play_by_play <- data.table::copy(dplyr::bind_rows(play_by_play))

	data.table::setkey(play_by_play, "ID")
	data.table::setkeyv(scores, c("Date", "Team_Home", "Team_Away"))


	print("Fittings game results...", quote = FALSE)
	pb <- txtProgressBar(min = 0, max = length(folders), style = 3)
	count <- 0
	for(fld in folders){

		if(!file.exists(paste(fld, "/Probabilities.rds", sep = "")) | file.exists(paste(fld, "/Results.rds", sep = ""))){

				next

		}


		probabilities <- readRDS(paste(fld, "/Probabilities.rds", sep = ""))

		data.table::setkeyv(probabilities, c("Date", "Team_Home", "Team_Away"))
		probabilities[scores, ID := i.ID]
		probabilities <- probabilities[!is.na(ID)]

		data.table::setkeyv(probabilities, c("ID", "Inn.", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2"))

		IDs = unique(probabilities$ID)

		for(id in IDs){

			out <- list()

			offers <- data.table::copy(probabilities[ID == id])
			formated_games <- data.table::copy(play_by_play[.(id), c("Inn.", "Score_Home", "Score_Away"), with = FALSE])

			formated_games[, Score_Total := Score_Home + Score_Away]
			formated_games[, Differential := Score_Home - Score_Away]
			formated_games[, Home_Is_Winning := Differential > 0]
			formated_games[, Is_Tied := Differential == 0]
			formated_games[, Total_Is_Even := Score_Total %% 2 == 0]

			#Get final scores per inning
			formated_games[, Is_Final := lapply(.SD, function(x){x == max(x)}), by = c("Inn."), .SDcols = "Score_Total"]
			formated_games <- unique(data.table::copy(formated_games[Is_Final == TRUE]))
			data.table::setorder(formated_games, "Inn.")

			#Scores per innings
			p_overtime <- as.numeric(any(formated_games[Inn. > 9, which = TRUE]))

			scores_per_inning <- list(first = formated_games[Inn. == 1],
										third = formated_games[Inn. == 3],
										fifth = formated_games[Inn. == 5],
										seventh = formated_games[Inn. == 7],
										final = formated_games[Inn. == max(Inn.)])

			scores_per_inning$reg <- scores_per_inning$final[Inn. == 9]
			scores_per_inning$overtime  <- scores_per_inning$final[Inn. > 9]


			probs <- lapply(scores_per_inning, function(x){

					temp <- x[Is_Tied == FALSE]
					sum(temp$Home_Is_Winning) / nrow(temp)

			})

			probs <- lapply(probs, function(x){

				temp <- t(as.matrix(c(x, 1 - x)))
				temp[which(is.na(temp))] <- -1

				data.table::as.data.table(temp)

			})


			probs$reg <- probs$reg * (1 - p_overtime)
			probs$overtime <- probs$overtime * p_overtime

			probs <- data.table::copy(dplyr::bind_rows(probs))
			probs[, Inn. := c(1, 3, 5, 7, 9, 100, 99)]

			n <- nrow(probs)
			probs <- rbind(probs[, c(3, 1)], probs[, c(3, 2)], use.names=FALSE)
			names(probs) <- c("Inn.", "P")

			probs[1:n, Bet_On2 := "Home"]
			probs[-c(1:n), Bet_On2 := "Away"]

			probs[1:n, Bet_On := scores[ID == id]$Team_Home]
			probs[-c(1:n), Bet_On := scores[ID == id]$Team_Away]			

			probs[, Bet_Type2 := 0.0]
			probs[, Bet_Type := "WINNER"]

			out$WINNER <- probs


			#ECART
			scores_per_inning$reg <- NULL
			scores_per_inning$overtime <- NULL

			spreads <- seq(from = -2.5, to = 2.5, by = 1)
			innings <- seq(from = 1, to = 9, by = 2)

			frames <- list()

			for(j in 1:length(scores_per_inning)){

				temp <- scores_per_inning[[j]]

				temp_list <- list()
				for(s in spreads){

					odds <- matrix(nrow = 1, ncol = 3)
					colnames(odds) <- c("Bet_Type2", "P_Home", "P_Away")

					odds[1] <- s
					odds[2] <- length(temp[Differential > s, which = TRUE]) / nrow(temp)
					odds[3] <- 1 - odds[2]

					temp_list[[length(temp_list) + 1]] <- data.table::as.data.table(odds)

				}
				names(temp_list) <- c(1:length(temp_list))
				temp_list <- data.table::copy(dplyr::bind_rows(temp_list))
				temp_list[, Inn. := innings[j]]

				frames[[length(frames) + 1]] <- temp_list
					
			}

			names(frames) <- c(1:length(frames))
			probs <- data.table::copy(dplyr::bind_rows(frames))

			n <- nrow(probs)
			probs <- data.table::copy(rbind(probs[, c(4, 1, 2)], probs[, c(4, 1, 3)], use.names=FALSE))
			names(probs) <- c("Inn.", "Bet_Type2", "P")

			probs[1:n, Bet_Type2 := -Bet_Type2]

			probs[1:n, Bet_On2 := "Home"]
			probs[-c(1:n), Bet_On2 := "Away"]

			probs[1:n, Bet_On := scores[ID == id]$Team_Home]
			probs[-c(1:n), Bet_On := scores[ID == id]$Team_Away]		

			probs[, Bet_Type := "SPREAD"]

			out$SPREAD <- probs



			#POINTS
			npoints <- seq(from = 0.5, to = 12, by = 0.5)

			totals <- list()
			for(j in 1:length(scores_per_inning)){

				temp <- scores_per_inning[[j]]

				temp_list <- list()
				for(s in npoints){

					odds <- matrix(nrow = 3, ncol = 3)
					colnames(odds) <- c("Bet_Type2", "P_Above", "P_Below")

					#Home
					odds[,1] <- s
					odds[1,2] <- length(temp[Score_Home > s, which = TRUE]) / nrow(temp)
					odds[1,3] <- length(temp[Score_Home < s, which = TRUE]) / nrow(temp)

					#Away
					odds[2,2] <- length(temp[Score_Away > s, which = TRUE]) / nrow(temp)
					odds[2,3] <- length(temp[Score_Away < s, which = TRUE]) / nrow(temp)

					#Total
					odds[3,2] <- length(temp[Score_Total > s, which = TRUE]) / nrow(temp)
					odds[3,3] <- length(temp[Score_Total < s, which = TRUE]) / nrow(temp)							

					temp_list[[length(temp_list) + 1]] <- cbind(c("Home", "Away", "None"), data.table::as.data.table(odds))
					names(temp_list[[length(temp_list)]])[1] <- c("Bet_On2")

				}
				names(temp_list) <- c(1:length(temp_list))
				temp_list <- data.table::copy(dplyr::bind_rows(temp_list))
				temp_list[, Inn. := innings[j]]

				totals[[length(totals) + 1]] <- temp_list
					
			}

			names(totals) <- c(1:length(totals))
			probs <- data.table::copy(dplyr::bind_rows(totals))
			probs[, Bet_Type := "POINTS"]

			nms <- names(probs)
			indices <- c(which(nms == "P_Above"), which(nms == "P_Below"))

			n <- nrow(probs)
			probs <- data.table::copy(rbind(probs[, -indices[2], with = FALSE], probs[, -indices[1], with = FALSE], use.names=FALSE))
			names(probs) <- nms[-indices[1]]
			names(probs)[indices[1]] <- "P"

			probs[-c(1:n), Bet_Type2 := Bet_Type2]

			probs[1:n, Bet_On := "Above"]
			probs[-c(1:n), Bet_On := "Below"]		

			probs[Bet_On2 == "None", Bet_Type := "SUM"]

			out$TOTAL <- probs	


			#ODD/EVEN
			frames <- list()

			for(j in 1:length(scores_per_inning)){

				temp <- scores_per_inning[[j]]

				odds <- matrix(nrow = 1, ncol = 3)
				colnames(odds) <- c("P_Even", "P_Odd", "Inn.")

				odds[1] <-  sum(temp$Total_Is_Even) / nrow(temp)
				odds[2] <- 1 - odds[1]
				odds[3] <- innings[j]

				frames[[length(frames) + 1]] <- as.data.table(odds)
					
			}

			names(frames) <- c(1:length(frames))
			probs <- data.table::copy(dplyr::bind_rows(frames))


			n <- nrow(probs)
			probs <- data.table::copy(rbind(probs[, c(3, 1)], probs[, c(3, 2)], use.names = FALSE))
			names(probs) <- c("Inn.", "P")

			probs[, Bet_Type := "ODD/EVEN"]
			probs[, Bet_Type2 := 0.0]

			probs[, Bet_On := "Odd"]
			probs[1:n, Bet_On := "Even"]
			probs[, Bet_On2 := "None"]	

			out$"ODD/EVEN" <- probs


			#MARGIN
			npoints <- c(1:6)

			marges <- list()
			for(j in 1:length(scores_per_inning)){

				temp <- scores_per_inning[[j]]

				temp_list <- list()
				for(s in npoints){

					odds <- matrix(nrow = 1, ncol = 4)
					colnames(odds) <- c("Inn.", "Bet_Type2", "P_Home", "P_Away")

					if(s < 6){

						odds[1] <- innings[j]
						odds[2] <- s
						odds[3] <- length(temp[Differential == s, which = TRUE]) / nrow(temp)
						odds[4] <- length(temp[-Differential == s, which = TRUE]) / nrow(temp)

					} else {

						odds[1] <- innings[j]
						odds[2] <- s
						odds[3] <- length(temp[Differential >= s, which = TRUE]) / nrow(temp)
						odds[4] <- length(temp[-Differential >= s, which = TRUE]) / nrow(temp)

					}


					temp_list[[length(temp_list) + 1]] <- data.table::as.data.table(odds)

				}

				marges[[length(marges) + 1]] <- data.table::copy(dplyr::bind_rows(temp_list))

					
			}	

			names(marges) <- c(1:length(marges))
			probs <- data.table::copy(dplyr::bind_rows(marges))

			n <- nrow(probs)
			probs <- data.table::copy(rbind(probs[, c(1, 2, 3)], probs[, c(1, 2, 4)], use.names = FALSE))
			names(probs) <- c("Inn.", "Bet_Type2", "P")

			probs[, Bet_Type := "MARGIN"]

			probs[1:n, Bet_On2 := "Home"]
			probs[-c(1:n), Bet_On2 := "Away"]

			probs[Bet_On2 == "Home", Bet_On := scores[ID == id]$Team_Home]
			probs[Bet_On2 == "Away", Bet_On := scores[ID == id]$Team_Away]


			out$MARGIN <- probs		


			#FIRST
			npoints <- c(2:7)
			first_to <- list()
			for(s in npoints){

				temp <- formated_games[Score_Home >= s | Score_Away >= s]
				if(nrow(temp) > 0){

					temp <- temp[, lapply(.SD, function(x){x[1]})]

				}
				
				odds <- matrix(nrow = 1, ncol = 4)
				colnames(odds) <- c("Bet_Type2", "P_Home", "P_Away", "P_None")

				odds[1] <- s
				odds[2] <- length(temp[Score_Home >= s & Score_Away < s, which = TRUE]) 
				odds[3] <- length(temp[Score_Home < s & Score_Away >= s, which = TRUE]) 
				odds[4] <- 1 - nrow(temp)

				#Ties
				if(sum(odds[2:4]) == 0){

					odds[2:4] <- c(-1, -1, 0)

				}

				odds <- rbind(odds[, c(1, 2)], odds[, c(1, 3)], odds[, c(1, 4)])	
				odds <- as.data.table(odds)
				odds[, Bet_On2 := c("Home", "Away", "None")]

				first_to[[length(first_to) + 1]] <- odds
					
			}

			names(first_to) <- c(1:length(first_to))
			probs <- data.table::copy(dplyr::bind_rows(first_to))
			names(probs)[2] <- "P"

			probs[Bet_On2 == "Home", Bet_On := scores[ID == id]$Team_Home]
			probs[Bet_On2 == "Away", Bet_On := scores[ID == id]$Team_Away]
			probs[Bet_On2 == "None", Bet_On := "None"]		

			probs[, Bet_Type := "FIRST"]

			probs[, Inn. := 9]

			out$FIRST <- probs

			out <- data.table::copy(dplyr::bind_rows(out))
			out[, ID := id]
			data.table::setkeyv(out, c("ID", "Inn.", "Bet_Type", "Bet_Type2", "Bet_On", "Bet_On2"))

			probabilities[out, Realization := i.P]

		}

		probabilities[Realization == 0, R := -1]
		probabilities[Realization == 1, R := Factor - 1]
		probabilities[Realization == -1, R := 0]

		saveRDS(probabilities[!is.na(Realization)], paste(fld, "/Results.rds", sep = ""))

		count <- count + 1
		setTxtProgressBar(pb, count)

	}

}



