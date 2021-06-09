#' Extract and process data needed to set up Markov Chains
#'
#' This function returns transition and event matrices, condition points distribution,
#' batting orders, ect. 
#'
#' Note: you need to initiate DB (.../MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds), 
#' scores (/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds) and play_by_play 
#' ("/MLB_Modeling/Scores/Clean_Data/Markov_Database.rds") prior to running this function.
#'
#'
#' @param date the date at which the match takes (or took) place
#' @param lineup a list of size 3 containing: home/away = list(bat = ..., pitch = ...), teams = list(home = ..., away =...). The most inner elements are vectors containing characters, i.e.: names.
#' @param params a list of size 3 containing: n_season = ..., n_match_bat = ..., n_match_pitch = ... . These are the time-windows used to compute averages.
#' @param id the match ID. Defaults to NULL; if specified, the lineup will be retrieved from historical files. (I.e.: use IDs to build a design matrix X post-webscrapping.)
#' @return A list of transition matrices
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#' @import pROC
#'


#Construct the lineup if an ID is provided instead of a lineup
Extract_Markov_Chain_Data <- function(date, lineup, params = list(n_season = 3,
																			n_match_bat = 400,
																			n_match_pitch = 400), 
													id = NULL,
													p_home_win = 0.5){

	if(!is.null(id)){

		id <- as.integer(id)

		keys <- data.table::key(scores)
		if(is.null(keys) | length(keys) > 1){

			data.table::setkey(scores, "ID")

		} else if(keys != "ID"){

			data.table::setkey(scores, "ID")

		}	

		game_info <- scores[.(id)]	
		teams <- unlist(game_info[, c("Team_Home", "Team_Away")])
		date <- game_info$Date
		season <- as.character(game_info$Season)

		for(team in teams){

			for(j in 1:2){

				keys <- data.table::key(DB[[season]][[team]][[j]])
				if(is.null(keys) | length(keys) > 1){

					data.table::setkey(DB[[season]][[team]][[j]], "ID")

				} else if(keys != "ID"){

					data.table::setkey(DB[[season]][[team]][[j]], "ID")

				}

			}

		}

		lineup <- list()
		for(a in 1:2){

			lineup[[a]] <- list()

			for(b in 1:2){

				lineup[[a]][[b]] <- DB[[season]][[teams[a]]][[b]][.(id)][Announced == "Yes"]$Name

			}
			names(lineup[[a]]) <- c("bat", "pitch")

		}
		lineup[[3]] <- list(home = as.character(teams[1]), away = as.character(teams[2]))
		names(lineup) <- c("home", "away", "teams")

	}

	season <- stringr::str_split(date, "-", simplify = TRUE)[1]

	seasons <- (as.integer(season) - params$n_season + 1):as.integer(season)
	seasons <- as.character(seasons)
	seasons <- seasons[which(seasons %in% names(play_by_play))]


	out <- list()


	get_transition_matrix <- function(data, weights = NULL, scaled = TRUE){

		out <- list()
		for(j in 1:2){

			out[[j]] <- matrix(0, nrow = nrow(play_by_play$states), ncol = nrow(play_by_play$states))
			rownames(out[[j]]) <- paste(paste(play_by_play$states$Base, play_by_play$states$Outs, sep = " ; "), "Out")
			colnames(out[[j]]) <- rownames(out[[j]])

		}

		names(out) <- c("n_events", "transition_mat")


		for(j in 1:nrow(data)){

			out$n_events[data$i[j], data$j[j]] <- out$n_events[data$i[j], data$j[j]] + 1

			if(!is.null(weights)){

				out$transition_mat[data$i[j], data$j[j]] <- out$transition_mat[data$i[j], data$j[j]] + as.numeric(data[, ..weights][j])

			} else {

				out$transition_mat[data$i[j], data$j[j]] <- out$transition_mat[data$i[j], data$j[j]] + 1

			}

		}


		for(i in 1:nrow(out$n_events)){

			t <- sum(out$n_events[i, ])
			if(t > 0){

				out$transition_mat[i, ] <- out$transition_mat[i, ] / t
				if(scaled){

					s <- sum(out$transition_mat[i, ])
					if(s != 1){

						out$transition_mat[i, ] <- out$transition_mat[i, ] / s

					}


				}

			}

		}

		out$transition_mat[nrow(out$transition_mat), nrow(out$transition_mat)] <- 1
		out$transition_mat[nrow(out$transition_mat), -nrow(out$transition_mat)] <- 0

		return(out$transition_mat)

	}

	output <- list()

	#Get the treshold for win/loss classification based on historical implied probabilities
	#Used to get the conditional markov chain for the favourite and the underdog

	

	
	auroc_obj <- pROC::roc(scores[!is.na(P_Home_Win_Historical)]$Win,
							scores[!is.na(P_Home_Win_Historical)]$P_Home_Win_Historical)

	treshold <- pROC::coords(auroc_obj, "best", transpose = TRUE)[1]

	scores[is.na(P_Home_Win_Historical), P_Home_Win_Historical := P_Estim]
	scores[, Is_Favourite := P_Home_Win_Historical >= treshold]

	for(k in 1:2){

		#Query the IDs needed
		frames <- list(bat = list(),
						pitch = list())

		b_names <- sapply(lineup[[k]]$bat, function(x){substr(x, 1, nchar(x) - 3)})
		p_names <- sapply(lineup[[k]]$pitch, function(x){substr(x, 1, nchar(x) - 3)})

		for(s in seasons){

			frames$bat[[length(frames$bat) + 1]] <- data.table::copy(play_by_play[[s]][Batter_Name2 %in% b_names | Batter_Announced == "No"])
			frames$pitch[[length(frames$pitch) + 1]] <- data.table::copy(play_by_play[[s]][Pitcher_Name2 %in% p_names])

			frames$bat[[length(frames$bat)]][Batter_Announced == "No", Batter_Name2 := "Random"]

		}


		for(j in 1:length(frames)){

			names(frames[[j]]) <- seasons

		}

		frames <- lapply(frames, function(x){data.table::copy(dplyr::bind_rows(x))})

		for(j in 1:length(frames)){

			data.table::setorderv(frames[[j]], "Date", -1)

		}


		#Tag OT innings as inning = 9
		for(i in 1:length(frames)){

			frames[[i]][Inn. > 9, Inn. := 9]

		}

		#Tag extra moves > 2 as 2
		for(i in 1:length(frames)){

			frames[[i]][Move_n > 2, Move_n. := 2]

		}


		#Conditonally subset the data based on P(Win)
		if(k == 1){

			p <- p_home_win

		} else {

			p <- 1 - p_home_win

		}


		for(j in 1:length(frames)){

			frames[[j]][is.na(P_Home_Win), P_Home_Win := P_Estim]

			frames[[j]][Team_Bat == Team_Home, P := P_Home_Win]
			frames[[j]][Team_Bat == Team_Away, P := 1 - P_Home_Win]

			frames[[j]][, Is_fav := P >= treshold]
			frames[[j]] <- frames[[j]][Is_fav == (p >= treshold)]

		}
	

		#Get the first n_matches per players
		frames$bat[, N_Match := lapply(.SD, function(x){

													m <- unique(x)
													return(match(x, m))

															}),

						by = c("Batter_Name2"),
						.SDcol = "ID"]			

		frames$bat <- frames$bat[N_Match <= params$n_match_bat]

		#Get the sample size per inning for each batters
		frames$bat[, Sample_Size := lapply(.SD, function(x){length(x)}),
						by = c("Inn.", "Batter_Name2"),
						.SDcol = "ID"]	


		#----------------------------------------------------------------------
		#Distribution of batting order per innings
		batting_orders <- list()
		data.table::setkeyv(frames$bat, c("Inn.", "Move_n"))
		no_outs <- play_by_play$states[Outs == 0, which = TRUE]

		rmv <- frames$bat[i %in% no_outs & j == nrow(play_by_play$states), which = TRUE]
		if(any(rmv)){

			frames$bat <- frames$bat[-rmv]

		}
		

		for(n in 1:9){

			fr <- data.table::copy(frames$bat[.(n, 1), c("ID", "Batter_Name2", "Batting_Order"), with = FALSE])

			#Marginal probabilities
			fr[, Marginal_P := lapply(.SD, function(x){

										v <- unique(x)
										l <- sapply(v, function(a){length(which(x == a))})

										index <- match(x, v)
										return(l[index] / length(x))

														}),
					by = "Batter_Name2",
					.SDcol = "Batting_Order"]

			fr[, ID := NULL]
			fr <- unique(fr)

			#Fix gaps
			tbf <- c(1:max(fr$Batting_Order))
			temp <- as.data.table(expand.grid(x = unique(fr$Batter_Name2), y = tbf))
			names(temp) <- c("Batter_Name2", "Batting_Order")
			temp[, Marginal_P := 0.0]

			data.table::setkeyv(temp, c("Batter_Name2", "Batting_Order"))
			data.table::setkeyv(fr, c("Batter_Name2", "Batting_Order"))

			temp[fr, Marginal_P := i.Marginal_P]
			fr <- data.table::copy(temp)


			data.table::setorderv(fr, c("Batter_Name2", "Batting_Order"))

			#Conditional probabilities
			#I.e.:
			#P(X = n | X >= n)

			fr[, Conditional_P := lapply(.SD, function(x){

													s <- sapply(c(1:length(x)), function(a){sum(x[a:length(x)])})
													return(x / s)

														}),
					by = "Batter_Name2",
					.SDcol = "Marginal_P"]

			fr[is.na(Conditional_P), Conditional_P := 0.0]

			fr[, Cdf := lapply(.SD, cumsum), by = "Batter_Name2", .SDcol = "Marginal_P"]


			batting_orders[[n]] <- data.table::copy(fr)
			

		}


		#----------------------------------------------------------------------
		#Probabilities for non-batting events (i.e.: stealing bases)

		additional_transition_probabilities <- list()
		data.table::setkey(frames$bat, "Inn.")
		for(n in 1:9){

			fr <- data.table::copy(frames$bat[.(n), c("Batter_Name2", "Move_n"), with = FALSE])
			fr[, Marginal_P := lapply(.SD, function(x){

										v <- unique(x)
										l <- sapply(v, function(a){length(which(x == a))})

										index <- match(x, v)
										return(l[index] / length(x))

														}),
					by = "Batter_Name2",
					.SDcol = "Move_n"]

			fr <- unique(fr[Move_n > 1])
			data.table::setorderv(fr, c("Batter_Name2"))

			additional_transition_probabilities[[n]] <- data.table::copy(fr)			

		}


		#----------------------------------------------------------------------
		#Transition matrices
		data.table::setkeyv(frames$bat, c("Inn.", "Move_n"))
		transition_matrices <- list()

		#avg_t_mat <- get_transition_matrix(frames$bat[Move_n == 1], weights = "Weights_Avg")
		avg_t_mat <- get_transition_matrix(frames$bat[Move_n == 1])
		for(i in 1:(nrow(play_by_play$states) - 1)){

				n_out <- play_by_play$states$Outs[i]
				no_access <- which(play_by_play$states$Outs < n_out)
				avg_t_mat[i, no_access] <- 0

				s <- sum(avg_t_mat[i, ])
				if(s > 0){

					avg_t_mat[i, ] <- avg_t_mat[i, ] / sum(avg_t_mat[i, ])

				}

		}

		#Verify that the markov chain converges
		eigenvals <- eigen(avg_t_mat)
		j <- which(Mod(eigenvals$values) == 1)

		v <- rep(0, nrow(avg_t_mat))
		v[1] <- 1

		d_mat <- diag(eigenvals$values)
		d_mat[-j] <- 0

		terminal_state <- round(Re(v %*% eigenvals$vectors %*% d_mat %*% solve(eigenvals$vectors)), 10)
		terminal_state <- terminal_state / sum(terminal_state)

		if(terminal_state[length(terminal_state)] != 1){

			print("Error: the average transition matrix over all innings doesn't converge to the absorbing state.", quote = FALSE)
			print("Returning NULL.", quote = FALSE)
			return(NULL)

		}


		pb <- txtProgressBar(min = 0, max = 9, style = 3)
		print(paste("Computing transition matrices for the", c("home", "visiting")[k], "team..."), quote = FALSE)
		for(n in 1:9){

			first_move <- data.table::copy(frames$bat[.(n, 1), c("Batter_Name2", "i", "j", "Points_scored_bat"), with = FALSE])
			extra_move <- data.table::copy(frames$bat[.(n, 2), c("Batter_Name2", "i", "j", "Points_scored_bat"), with = FALSE])

			players <- unique(first_move$Batter_Name2)
			nth_event <- c(1:max(batting_orders[[n]]$Batting_Order))

			ordered_transitions <- list()
			ordered_extra_moves <- list()

			#-----------------------------------------
			#transition_matrices_per_player <- lapply(players, function(p){get_transition_matrix(first_move[Batter_Name2 == p], weights = "Weights")})
			transition_matrices_per_player <- lapply(players, function(p){get_transition_matrix(first_move[Batter_Name2 == p])})
			names(transition_matrices_per_player) <- players

			#extra_move_per_player <- lapply(players, function(p){get_transition_matrix(extra_move[Batter_Name2 == p], weights = "Weights")})
			extra_move_per_player <- lapply(players, function(p){get_transition_matrix(extra_move[Batter_Name2 == p])})
			names(extra_move_per_player) <- players			

			#Matrices for 1st, 2nd, ... , n events occuring during the inning
			#Where events are batters vs pitchers (i.e.: no base steal, ect.)
			#Then extra moves
			rmv <- c()
			no_convergence <- c()
			for(event in nth_event){

				t_mat <- list(first_move = matrix(0, nrow = nrow(play_by_play$states), ncol = nrow(play_by_play$states)),
								extra_move = matrix(0, nrow = nrow(play_by_play$states), ncol = nrow(play_by_play$states)))

				normalizing_constants <- list(first_move = 0,
												extra_move = 0)

				#--------------------------------------------
				#BATTER VS PITCHER
				for(p in players){

					w <- batting_orders[[n]][Batter_Name2 == p & Batting_Order == event]
					if(nrow(w) == 0){

						next

					}

					w <- w$Conditional_P[1]
					t <- transition_matrices_per_player[[p]]

					t_mat$first_move <- t_mat$first_move + w * t
					normalizing_constants$first_move <- normalizing_constants$first_move + w	

				}

				if(normalizing_constants$first_move > 0){

					t_mat$first_move <- t_mat$first_move / normalizing_constants$first_move

				} else {

					rmv <- c(rmv, event)

				}


				#--------------------------------------------
				#EXTRA MOVE
				sum_w <- 0
				for(p in players){

					data <- extra_move[Batter_Name2 == p]
					if(nrow(data) == 0){

						next

					}

					w <- batting_orders[[n]][Batter_Name2 == p & Batting_Order == event]
					if(nrow(w) == 0){

						next

					}

					w <- w$Cdf[1]
					sum_w <- sum_w + w

					w <- w * additional_transition_probabilities[[n]][Batter_Name2 == p]$Marginal_P[1]

					t <- extra_move_per_player[[p]]

					t_mat$extra_move <- t_mat$extra_move + w * t
					normalizing_constants$extra_move <- normalizing_constants$extra_move + w	

				}

				if(normalizing_constants$extra_move > 0){

					t_mat$extra_move <- t_mat$extra_move/normalizing_constants$extra_move
					w <- normalizing_constants$extra_move/sum_w

					t_mat$extra_move <- w * t_mat$extra_move + (1 - w) * diag(rep(1, nrow(t_mat$extra_move)))

				} else {

					t_mat$extra_move <- diag(rep(1, nrow(t_mat$extra_move)))

				}

				for(j in 1:nrow(t_mat$first_move)){

					s <- sum(t_mat$first_move[j, ])
					if(s > 0){

						t_mat$first_move[j, ] <- t_mat$first_move[j, ] / s

					} 

					s <- sum(t_mat$extra_move[j, ])
					if(s > 0){

						t_mat$extra_move[j, ] <- t_mat$extra_move[j, ] / s

					}

				}

				ordered_transitions[[event]] <- t_mat$first_move 
				ordered_extra_moves[[event]] <- t_mat$extra_move

			}

			if(length(rmv) > 0){

				ordered_transitions <- ordered_transitions[-rmv]
				ordered_extra_moves <- ordered_extra_moves[-rmv]

				if(length(ordered_transitions) <= 4){

					print("Error: the chain cannot be reasonably be approximated.", quote = FALSE)
					print("Returning NULL", quote = FALSE)
					return(NULL)

				}

			}


			#Prevent transitions of the type out_i+1 < out_i
			for(i in 1:(nrow(play_by_play$states) - 1)){

				n_out <- play_by_play$states$Outs[i]
				no_access <- which(play_by_play$states$Outs < n_out)	

				if(length(no_access) > 0){

					for(j in 1:length(ordered_transitions)){

						ordered_transitions[[j]][i, no_access] <- 0
						s <- sum(ordered_transitions[[j]][i, ])

						if(s > 0){

							ordered_transitions[[j]][i, ] <- ordered_transitions[[j]][i, ] / s

						}

						#2 outs and P(inning over) = 0 gets replaced with the average
						if(n_out == 2 & ordered_transitions[[j]][i, nrow(ordered_transitions[[j]])] == 0 & s != 0){

							ordered_transitions[[j]][i, ] <- avg_t_mat[i, ]

						}

						ordered_extra_moves[[j]][i, no_access] <- 0
						s <- sum(ordered_extra_moves[[j]][i, ])

						if(s > 0){

							ordered_extra_moves[[j]][i, ] <- ordered_extra_moves[[j]][i, ] / s

						} else {

							ordered_extra_moves[[j]][i, i] <- 1

						}

					}


				}

			}


			#Replace rows belonging to never-visited states with the average accross all innings
			for(j in 1:length(ordered_transitions)){

				n_access <- apply(ordered_transitions[[j]], 1, function(x){length(which(x != 0))})
				rmv <- which(n_access == 0)
				if(length(rmv) > 0){

					ordered_transitions[[j]][rmv, ] <- avg_t_mat[rmv, ]

				}

			}

			#Replace absorbing states with the average accross all innings
			for(j in 1:length(ordered_transitions)){

				single_issue <- apply(ordered_transitions[[j]], 1, function(x){

										if(any(which(x == 1))){which(x == 1)[1]} else {NA}

				})

				index <- which(!is.na(single_issue))
				if(length(index) == 1){next}

				single_issue <- cbind(index, single_issue[index])
				single_issue <- single_issue[-nrow(single_issue), ]
				index <- index[-length(index)]
				if(class(single_issue)[1] != "matrix"){

					single_issue <-  t(as.matrix(single_issue))

				}

				absorbing_state <- which(single_issue[, 1] == single_issue[, 2])
				if(any(absorbing_state)){

					for(m in absorbing_state){

						ordered_transitions[[j]][index[m], ] <- avg_t_mat[index[m], ]

					}

				}
				

				if(length(rmv) > 0){

					ordered_transitions[[j]][rmv, ] <- avg_t_mat[rmv, ]

				}

			}			

			names(ordered_transitions) <- c(1:length(ordered_transitions))
			names(ordered_extra_moves) <- c(1:length(ordered_transitions))

			#Verify that the markov chain converges, for each step of the inning
			no_convergence <- rep(FALSE, length(ordered_transitions))

			M <- ordered_transitions[[1]] %*% ordered_extra_moves[[1]]
			for(j in 2:4){

				M <- M %*% ordered_transitions[[j]] %*% ordered_extra_moves[[j]]

			}


			for(j in 4:length(ordered_transitions)){

				A <- M 
				#Obtain odds for 2^5 full rotations, which should occur with P ~ 0
				for(j in 1:5){A <- A %*% A}

				v <- rep(0, nrow(avg_t_mat))
				v[1] <- 1

				terminal_state <- round(v %*% A, 40)
				terminal_state <- terminal_state / sum(terminal_state)

				no_convergence[j] <- terminal_state[length(terminal_state)] != 1

				if(j < length(ordered_transitions)){

					M <- M %*% ordered_transitions[[j + 1]] %*% ordered_extra_moves[[j + 1]]

				}

			}

			if(any(no_convergence)){

				print(paste("Fixing non-converging steps for Inn. #", n, sep = ""), quote = FALSE)

				while(any(no_convergence)){

					fix <- which(no_convergence)[1]
					avaible <- which(!no_convergence)
					closest <- order(abs(avaible - fix))
					closest <- closest[c(1:min(3, length(closest)))]

					if(length(closest) == 1){

						print("Fix failed; return NULL", quote = FALSE)
						return(NULL)

					}

					nmat <- length(closest)
					closest <- closest[closest != fix]
					for(j in closest){

						ordered_transitions[[fix]] <- ordered_transitions[[fix]] + ordered_transitions[[j]]
						ordered_extra_moves[[fix]] <- ordered_extra_moves[[fix]] + ordered_extra_moves[[j]]

					}

					ordered_transitions[[fix]] <- ordered_transitions[[fix]] / nmat
					ordered_extra_moves[[fix]] <- ordered_extra_moves[[fix]] / nmat

					no_convergence[fix] <- FALSE

				}

			}

			#Re-check the final chain for convergence after the fix
			M <- ordered_transitions[[1]] %*% ordered_extra_moves[[1]]
			for(j in 2:length(ordered_transitions)){

				M <- M %*% ordered_transitions[[j]] %*% ordered_extra_moves[[j]]

			}
			#Obtain odds for 32 full rotations, which should occur with P ~ 0
			for(j in 1:5){M <- M %*% M}
	
			v <- rep(0, nrow(avg_t_mat))
			v[1] <- 1

			terminal_state <- round(v %*% M, 40)
			terminal_state <- terminal_state / sum(terminal_state)

			if(terminal_state[length(terminal_state)] != 1){

				if(n == 1){

					print(paste("Error: the transition matrix of the ", n, "st inning doesn't converge."), quote = FALSE)

				} else {

					print(paste("Error: the transition matrix of the ", n, "th inning doesn't converge."), quote = FALSE)

				}
				
				print("Returning NULL.", quote = FALSE)
				return(NULL)

			}			


			transition_matrices[[n]] <- list(first = ordered_transitions,
												extra = ordered_extra_moves)

			setTxtProgressBar(pb, n)

		}

		names(transition_matrices) <- c(1:9)

		output[[k]] <- transition_matrices

	}

	names(output) <- c("home", "away")

	return(output)

}