#' Performs simulations and returns the estimated probabilies for avaible bets.
#'
#' This function computes the estimates needed to identify arbitrage opportunities.
#'
#'
#'
#' @param path the path used by the webscrapper to save data
#' @param n_simulations numbers of simulations
#' @param ncores numbers of cores to be used during the simulations
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#' @import doParallel
#' @import foreach
#' @import doParallel
#' @import iterators
#' @import parallel
#'


Compute_Fair_Factors <- function(path, n_simulations = NULL, ncores = NULL){

	if(is.null(n_simulations)){

		#Choose n such that the marging of error (95% CI, two-sided) is at +/- 0.01
		#I.e.: [p - 0.01, p + 0.01]
		n_simulations <- ceiling((qnorm(0.975) * 0.5 / 0.01)^2)

	}


	play_by_play <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/Markov_Database.rds", sep = ""))
	DB <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds", sep = ""))
	scores <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds", sep = ""))

	folder_directory <- paste(path, "/MLB_Modeling/Betting/Predicted_Lineups", sep = "")
	path_check <- paste(folder_directory, "/Betting_Database.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("File missing at:", path_check, sep = ""), quote = FALSE)

	}
	bets <- readRDS(path_check)

	folders <- list.dirs(path = folder_directory, full.names = TRUE, recursive = TRUE)[-1]
	if(length(folders) == 0){

		print("Error: no bet webscrapped yet.", quote = FALSE)
		print(paste("Missing files should be located at:", folder_directory), quote = FALSE)
		return(NULL)

	}

	for(fld in folders){

		date <- stringr::str_split(fld, "/")[[1]]
		date <- as.Date(date[length(date)], "%d-%m-%Y")

		if(file.exists(paste(fld, "/Simulated_Games.rds", sep = "")) & date != Sys.Date()){

			next

		}

		lineups_path <- list(bat = paste(fld, "/Bat.csv", sep = ""),
								pitch = paste(fld, "/Pitch.csv", sep = ""))

		files_exist <- lapply(lineups_path, file.exists)
		if(any(!unlist(files_exist))){

			print("Error: no lineups found.", quote = FALSE)
			print("Moving on to the next folder.", quote = FALSE)
			next

		}

		lineups_bat <- data.table::fread(paste(fld, "/Bat.csv", sep = ""))
		lineups_pitch <- data.table::fread(paste(fld, "/Pitch.csv", sep = ""))

		offers <- data.table::copy(bets[Date == date])

		offers[, Most_Recent := lapply(.SD, function(x){x == min(x)}),
					by = c("Team_Home", "Team_Away"),
					.SDcols = "Minutes_Until_Start"]
		offers <- offers[Most_Recent == TRUE]			

		matches <- unique(offers[, c("Team_Home", "Team_Away", "Game_Time")])

		#Fix missing team names
		rmv <- which(matches$Team_Home == "" | matches$Team_Away == "")
		if(any(rmv)){

			matches <- matches[-rmv]

			data.table::setkey(offers, "Team_Home")
			data.table::setkey(matches, "Team_Home")
			offers[matches, Team_Away := i.Team_Away]

			data.table::setkey(offers, "Team_Away")
			data.table::setkey(matches, "Team_Away")
			offers[matches, Team_Home := i.Team_Home]

			offers[, Most_Recent := lapply(.SD, function(x){x == min(x)}),
						by = c("Team_Home", "Team_Away"),
						.SDcols = "Minutes_Until_Start"]
			offers <- offers[Most_Recent == TRUE]	

			matches <- unique(offers[, c("Team_Home", "Team_Away", "Game_Time")])

		}


		#Check for any duplicated bet
		offers[, Is_Unique := lapply(.SD, function(x){


										if(length(x) == 1){TRUE} else {

											x2 <- rep(FALSE, length(x))
											x2[which.min(x)] <- TRUE

										}


						}),
						by = c("Date", "Team_Home", "Team_Away", "Bet_On", "Bet_On2", "Bet_Type", "Bet_Type2", "Inn."),
						.SDcols = "Factor"]

		offers <- offers[Is_Unique == TRUE]	
		offers[, Is_Unique := NULL]		


		results <- list()
		simulations <- list()

		print(paste("Scrapping:", date), quote = FALSE)
		print(paste("Number of matches to process:", nrow(matches)), quote = FALSE)
		for(i in 1:nrow(matches)){

			#Retrieve parameters
			lineup <- list(home = list(bat = lineups_bat[Team == matches$Team_Home[i]]$Name,
										pitch = lineups_pitch[Team == matches$Team_Home[i]]$Name),
							away = list(bat = lineups_bat[Team == matches$Team_Away[i]]$Name,
										pitch = lineups_pitch[Team == matches$Team_Away[i]]$Name),
							teams = list(home = matches$Team_Home[i],
											away = matches$Team_Away[i]))

			is_empty <- lapply(lineup[1:2], function(x){length(x[[1]]) == 0})
			if(any(unlist(is_empty))){

				print("Error: no lineups found for specified teams.", quote = FALSE)
				print("Moving on to the next team.", quote = FALSE)

			}



			ml <- c(offers[Team_Home == lineup$teams$home & Bet_On2 == "Home" & Bet_Type == "WINNER" & Inn. == 9]$Factor[1],
					offers[Team_Away == lineup$teams$away & Bet_On2 == "Away" & Bet_Type == "WINNER" & Inn. == 9]$Factor[1])

			b <- ml[1] + ml[2] - 2
			c <- ml[1] * ml[2] - (ml[1] + ml[2])

			margin <- (1/2) * (-b + c(1,-1) * sqrt(b^2 - 4*c))
			margin <- min(margin[which(margin >= 0)])

			p_home_win <- 1 / (ml[1] + margin)

			#Transition matrices
			print("Computing transition matrices...", quote = FALSE)
			markov <- Extract_Markov_Chain_Data(date = date, lineup = lineup, p_home_win = p_home_win)
			#Failure to converge -> skip
			if(is.null(markov) | class(markov)[1] == "try-error"){

				next

			}


			#Simulations
			if(is.null(ncores)){

				ncores <- floor(detectCores() / 2)
				print(paste("Using", ncores, "cores."), quote = FALSE)

			}

			print("Simmulating games for:", quote = FALSE)
			print(matches[i])

			cl <- makeCluster(ncores)
			registerDoParallel(cl)

				simulated_matches <- foreach(j = 1:n_simulations) %dopar% {simmulate_game()}

			stopCluster(cl)

			print("Formating games...", quote = FALSE)
			formated_games <- lapply(simulated_matches, function(x){

				home <- unique(x$home[, c("Inn.", "Points"), with = FALSE])
				home <- home[, lapply(.SD, max), by = "Inn.", .SDcols = "Points"]

				away <- unique(x$away[, c("Inn.", "Points"), with = FALSE])
				away <- away[, lapply(.SD, max), by = "Inn.", .SDcols = "Points"]

				home[, Score_Away := away$Points]
				names(home)[which(names(home) == "Points")] <- "Score_Home"

				home[, Score_Total := Score_Home + Score_Away]
				home[, Differential := Score_Home - Score_Away]

				home[, Home_Is_Winning := Differential > 0]
				home[, Is_Tied := Differential == 0]
				home[, Total_Is_Even := Score_Total %% 2 == 0]

				return(home)

			})

			for(j in 1:length(formated_games)){

				formated_games[[j]][, Game := j]

			}

			names(formated_games) <- c(1:length(formated_games))
			formated_games <- dplyr::bind_rows(formated_games)

			#Output
			out <- list()

			#GAGNANT
			p_overtime <- length(which(formated_games$Inn. == 10)) / n_simulations

			#Final scores per innings
			scores_per_inning <- list(first = formated_games[Inn. == 1],
										third = formated_games[Inn. == 3],
										fifth = formated_games[Inn. == 5],
										seventh = formated_games[Inn. == 7],
										final = formated_games[formated_games[, .I[Inn. == max(Inn.)], by = Game]$V1])

			scores_per_inning$reg <- scores_per_inning$final[Inn. == 9]
			scores_per_inning$overtime  <- scores_per_inning$final[Inn. > 9]

			probs <- lapply(scores_per_inning, function(x){

					temp <- c(sum(x$Home_Is_Winning) / nrow(x), sum(x$Is_Tied) / nrow(x))
					temp <- c(temp[1], 1 - sum(temp), temp[2])

					data.table::as.data.table(t(as.matrix(temp)))

			})

			probs$reg <- probs$reg * (1 - p_overtime)
			probs$reg$V3 <- p_overtime
			probs$overtime <- probs$overtime * p_overtime

			probs <- data.table::copy(dplyr::bind_rows(probs))
			probs[, Inn. := c(1, 3, 5, 7, 9, 100, 99)]

			n <- nrow(probs)
			probs <- rbind(probs[, c(4, 1, 3)], probs[, c(4, 2, 3)], use.names=FALSE)
			names(probs) <- c("Inn.", "P", "Push")

			probs[1:n, Bet_On2 := "Home"]
			probs[-c(1:n), Bet_On2 := "Away"]

			probs[1:n, Bet_On := lineup$teams$home]
			probs[-c(1:n), Bet_On := lineup$teams$away]			

			probs[, Bet_Type2 := 0.0]
			probs[, F := 1/P]
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

			probs[1:n, Bet_On := lineup$teams$home]
			probs[-c(1:n), Bet_On := lineup$teams$away]			

			probs[, F := 1/P]
			probs[, Bet_Type := "SPREAD"]

			probs[, Push := 0]

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

			probs[, F := 1/P]
			probs[Bet_On2 == "None", Bet_Type := "SUM"]

			probs[, Push := 0]

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

			probs[, F := 1/P]

			probs[, Push := 0]

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

			probs[Bet_On2 == "Home", Bet_On := lineup$teams$home]
			probs[Bet_On2 == "Away", Bet_On := lineup$teams$away]

			probs[, F := 1/P]

			probs[, Push := 0]

			out$MARGIN <- probs


			#FIRST
			npoints <- c(2:7)
			first_to <- list()
			for(s in npoints){

				temp <- formated_games[Score_Home >= s | Score_Away >= s]
				temp <- temp[, lapply(.SD, function(x){x[1]}), by = c("Game")]

				odds <- matrix(nrow = 1, ncol = 4)
				colnames(odds) <- c("Bet_Type2", "P_Home", "P_Away", "P_None")

				odds[1] <- s
				odds[2] <- length(temp[Score_Home >= s & Score_Away < s, which = TRUE]) / n_simulations
				odds[3] <- length(temp[Score_Home < s & Score_Away >= s, which = TRUE]) / n_simulations 
				odds[4] <- 1 - nrow(temp) / n_simulations

				push <- 1 - sum(odds[2:4])

				odds <- rbind(odds[, c(1, 2)], odds[, c(1, 3)], odds[, c(1, 4)])	
				odds <- as.data.table(odds)
				odds[, Bet_On2 := c("Home", "Away", "None")]
				odds[, Push := 0.0]
				odds[Bet_On2 != "None", Push := eval(push)]


				first_to[[length(first_to) + 1]] <- odds
					
			}

			names(first_to) <- c(1:length(first_to))
			probs <- data.table::copy(dplyr::bind_rows(first_to))
			names(probs)[2] <- "P"

			probs[Bet_On2 == "Home", Bet_On := lineup$teams$home]
			probs[Bet_On2 == "Away", Bet_On := lineup$teams$away]
			probs[Bet_On2 == "None", Bet_On := "None"]		

			probs[, Bet_Type := "FIRST"]
			probs[, F := 1/P]

			probs[, Inn. := 9]

			out$FIRST <- probs

			out <- data.table::copy(dplyr::bind_rows(out))
			out[, Team_Home := lineup$teams$home]
			out[, Team_Away := lineup$teams$away]

			data.table::setkeyv(out, intersect(names(offers), names(out)))
			data.table::setkeyv(offers, intersect(names(offers), names(out)))
			out <- out[offers, nomatch = 0]

			out[, E_R := P * (Factor - 1) + (1 - P - Push) * (-1)]
			out[, Var_R := P * (Factor - 1)^2 + (1 - P - Push) * 1]
			out[, Sdev_R := sqrt(Var_R)]
			out[, Sharpe := E_R / Sdev_R]

			out[, Estimator_Sdev := sqrt(P * (1 - P) / n_simulations)]
			out[, P_Lower_Bound_95 := P - qnorm(0.975) * Estimator_Sdev]
			out[, F_Upper_Bound_95 := ceiling((1 / P_Lower_Bound_95) * 100) / 100]

			out[, E_R_Lower_Bound_95 := P_Lower_Bound_95 * (Factor - 1) + (1 - P_Lower_Bound_95 - Push) * (-1)]
			out[, Var_R_Lower_Bound_95 := P_Lower_Bound_95 * (Factor - 1)^2 + (1 - P_Lower_Bound_95 - Push) * 1]
			out[, Sdev_R_Lower_Bound_95 := sqrt(Var_R_Lower_Bound_95)]
			out[, Sharpe_Lower_Bound_95 := E_R_Lower_Bound_95 / Sdev_R_Lower_Bound_95]	
			
			#Compute Kelly criterion
			out[Push == 0, Kelly := (P * Factor - 1) / (Factor - 1)]
			#For multiple outcomes
			f <- function(win, push, F){

				p <- c(win, push, 1 - win - push)
				r <- c(F - 1, 0, -1)

				obj_fun <- function(x){

					sum(p*r / (1 + b*x))

				}

				return(optim(par = 0.5, fn = obj_fun, lower = 0, upper = 1, method = "L-BFGS-B")$value)

			}

			index <- out[Push != 0, which = TRUE]
			temp <- as.matrix(out[index, c("P", "Push", "Factor")])
			temp <- apply(temp, 1, function(x){f(x[1], x[2], x[3])})

			out[index, Kelly := c(temp)]

			out[Push == 0, Kelly_Lower_Bound_95 := (P_Lower_Bound_95 * Factor - 1) / (Factor - 1)]

			temp <- as.matrix(out[index, c("P_Lower_Bound_95", "Push", "Factor")])
			temp <- apply(temp, 1, function(x){f(x[1], x[2], x[3])})

			out[index, Kelly_Lower_Bound_95 := c(temp)]


			#Tag best bets per category (i.e.: win in reg, OT or match)
			out[Kelly > 0, Best_Kelly := lapply(.SD, function(x){x == max(x)}), 
						.SDcols = "Kelly"]	

			#Tag best bets per category (i.e.: win in reg, OT or match)
			out[Kelly > 0, Best_Kelly_Lower_Bound_95 := lapply(.SD, function(x){x == max(x)}), 
						.SDcols = "Kelly_Lower_Bound_95"]	

			#Tag best bets per category (i.e.: win in reg, OT or match)
			out[Kelly > 0, Best_Sharpe := lapply(.SD, function(x){x == max(x)}), 
						.SDcols = "Sharpe"]	

			#Tag best bets per category (i.e.: win in reg, OT or match)
			out[Kelly > 0, Best_Sharpe_Lower_Bound_95 := lapply(.SD, function(x){x == max(x)}), 
						.SDcols = "Sharpe_Lower_Bound_95"]	


			data.table::setorderv(out, c("Inn.", "Bet_Type", "Bet_Type2"))

			results[[length(results) + 1]] <- out
			simulations[[length(simulations) + 1]] <- formated_games

			print("--------------------------------------------------------", quote = FALSE)
			

		}

		
		for(j in 1:length(results)){

			names(simulations)[j] <- paste(results[[j]]$Team_Away[1], "at", results[[j]]$Team_Home[1])

		}

		names(results) <- c(1:length(results))
		results <- data.table::copy(dplyr::bind_rows(results))

		saveRDS(formated_games, paste(fld, "/Simulated_Games.rds", sep = ""))
		saveRDS(results, paste(fld, "/Probabilities.rds", sep = ""))

	}


}





