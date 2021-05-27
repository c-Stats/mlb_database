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
#' @return A single row of the design matrix X given two teams and two rosters.
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


#Construct the lineup if an ID is provided instead of a lineup
Extract_Markov_Chain_Data <- function(date, lineup, params, id = NULL){

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


	get_matrices <- function(data){

		out <- list()
		for(j in 1:2){

			out[[j]] <- matrix(0, nrow = nrow(play_by_play$states), ncol = nrow(play_by_play$states))
			rownames(out[[j]]) <- paste(paste(play_by_play$states$Base, play_by_play$states$Outs, sep = " ; "), "Out")
			colnames(out[[j]]) <- rownames(out[[j]])

		}

		names(out) <- c("n_events", "transition_mat")


		for(j in 1:nrow(data)){

			out$n_events[data$i[j], data$j[j]] <- out$n_events[data$i[j], data$j[j]] + 1
			out$transition_mat[data$i[j], data$j[j]] <- out$transition_mat[data$i[j], data$j[j]] + 1

		}


		for(i in 1:nrow(out$n_events)){

			t <- sum(out$n_events[i, ])
			if(t > 0){

				out$transition_mat[i, ] <- out$transition_mat[i, ] / t

			}

		}

		out$points_distribution <- data[, names(data), with = FALSE]
		out$points_distribution[, n := lapply(.SD, length), by = c("i", "j", "Move_n", "Points_scored_bat"), .SDcol = "Points_scored_bat"]

		out$points_distribution <- unique(out$points_distribution[, c("i", "j", "Move_n", "Points_scored_bat", "n")])
		out$points_distribution[, N := lapply(.SD, sum), by = c("i", "j", "Move_n"), .SDcol = "n"]
		out$points_distribution[, p := lapply(.SD, function(x){x / sum(x)}), by = c("i", "j", "Move_n"), .SDcol = "n"]

		data.table::setorderv(out$points_distribution, c("i", "j", "Move_n"))


		out$batting_order <- data[, names(data), with = FALSE]
		out$batting_order[, n := lapply(.SD, length), by = c("Inn.", "Move_n", "Batting_Order"), .SDcol = "Batting_Order"]
		
		out$batting_order <- unique(out$batting_order[, c("Inn.", "Move_n", "Batting_Order", "n")])
		out$batting_order[, N := lapply(.SD, sum), by = c("Inn.", "Move_n"), .SDcol = "n"]
		out$batting_order[, p := lapply(.SD, function(x){x / sum(x)}), by = c("Inn.", "Move_n"), .SDcol = "n"]

		data.table::setorderv(out$batting_order, c("Inn.", "Move_n", "Batting_Order"))	

		out$batters_faced <- unique(data[, c("ID", "Batters_Faced"), with = FALSE])

		return(out)

	}

	output <- list()

	for(k in 1:2){

		#Query the IDs needed
		frames <- list(bat = list(),
						pitch = list())

		b_names <- sapply(lineup[[k]]$bat, function(x){substr(x, 1, nchar(x) - 3)})
		p_names <- sapply(lineup[[k]]$pitch, function(x){substr(x, 1, nchar(x) - 3)})

		for(s in seasons){

			frames$bat[[length(frames$bat) + 1]] <- play_by_play[[s]][Batter_Name2 %in% b_names]
			frames$pitch[[length(frames$pitch) + 1]] <- play_by_play[[s]][Pitcher_Name2 %in% p_names]

		}

		for(j in 1:2){

			names(frames[[j]]) <- seasons

		}

		frames <- lapply(frames, dplyr::bind_rows)

		for(j in 1:2){

			data.table::setorderv(frames[[j]], "Date", -1)

		}


		#Announced players
		event_matrices <- list(bat = list(),
								pitch = list())

		for(b in b_names){

			if(k == 1){

				temp <- frames$bat[Batter_Name2 == b & Team_Home == Team_Bat]

			} else {

				temp <- frames$bat[Batter_Name2 == b & Team_Away == Team_Bat]

			}
			
			matches <- unique(temp$ID)
			matches <- matches[c(1:min(params$n_match_bat, length(matches)))]

			temp <- temp[ID %in% matches]

			matrices <- list()
			innings <- sort(unique(temp$Inn.))

			for(inn in innings){

				temp2 <- temp[Inn. == inn]
				Move_n <- sort(unique(temp2$Move_n))

				out <- list()

				for(move in Move_n){

					out[[length(out) + 1]] <- get_matrices(temp2[Move_n == move])

				}

				names(out) <- Move_n
				matrices[[length(matrices) + 1]] <- out

			}

			names(matrices) <- innings
			event_matrices$bat[[length(event_matrices$bat) + 1]] <- matrices

			#Info frame for weighting, ect.
			info <- unique(temp[, c("Inn.", "Move_n")])
			temp[, Count := lapply(.SD, length), by = c("Inn.", "Move_n"), .SDcol = "Move_n"]

			data.table::setkeyv(temp, c("Inn.", "Move_n"))
			data.table::setkeyv(info, c("Inn.", "Move_n"))

			info[temp, Count := i.Count]
			info[, N_Games := length(matches)]

			data.table::setorderv(info, c("Inn.", "Move_n"))

			event_matrices$bat[[length(event_matrices$bat)]]$n_events <- info
			
		}

		names(event_matrices$bat) <- lineup[[k]]$bat


		for(b in p_names){

			if(k == 1){

				temp <- frames$pitch[Pitcher_Name2 == b & Team_Home == Team_Pitch]

			} else {

				temp <- frames$pitch[Pitcher_Name2 == b & Team_Away == Team_Pitch]

			}
			
			matches <- unique(temp$ID)
			matches <- matches[c(1:min(params$n_match_pitch, length(matches)))]

			temp <- temp[ID %in% matches]

			matrices <- list()
			innings <- sort(unique(temp$Inn.))

			for(inn in innings){

				temp2 <- temp[Inn. == inn]
				Move_n <- sort(unique(temp2$Move_n))

				out <- list()

				for(move in Move_n){

					out[[length(out) + 1]] <- get_matrices(temp2[Move_n == move])

				}

				names(out) <- Move_n
				matrices[[length(matrices) + 1]] <- out

			}

			names(matrices) <- innings
			event_matrices$pitch[[length(event_matrices$pitch) + 1]] <- matrices

			#Info frame for weighting, ect.
			info <- unique(temp[, c("Inn.", "Move_n")])
			temp[, Count := lapply(.SD, length), by = c("Inn.", "Move_n"), .SDcol = "Move_n"]

			data.table::setkeyv(temp, c("Inn.", "Move_n"))
			data.table::setkeyv(info, c("Inn.", "Move_n"))

			info[temp, Count := i.Count]
			info[, N_Games := length(matches)]

			data.table::setorderv(info, c("Inn.", "Move_n"))

			event_matrices$pitch[[length(event_matrices$pitch)]]$n_events <- info

		}

		names(event_matrices$pitch) <- lineup[[k]]$pitch


		#Unannounced players (random player effect)
		event_matrices_UA <- list()


		if(k == 1){

			last_matches <- scores[Date < date & Team_Home == teams[1]]

		} else {

			last_matches <- scores[Date < date & Team_Away == teams[2]]

		}
		
		data.table::setorder(last_matches, -Date)
		u_id <- unique(last_matches$ID)

		last_id <- list(bat = u_id[c(1:min(params$n_match_bat, length(u_id)))],
							pitch = u_id[c(1:min(params$n_match_pitch, length(u_id)))])


		fillers <- list(bat = list(),
							pitch = list())

		for(s in seasons){

			fillers$bat[[length(fillers$bat) + 1]] <- DB[[s]][[teams[k]]]$Bat_Relief[ID %in% last_id$bat]
			fillers$bat[[length(fillers$bat)]] <- fillers$bat[[length(fillers$bat)]][!(Name %in% lineup[[k]]$bat)]
			fillers$bat[[length(fillers$bat)]] <- unique(fillers$bat[[length(fillers$bat)]]$Name)

			fillers$pitch[[length(fillers$pitch) + 1]] <- DB[[s]][[teams[k]]]$Pitch_Relief[ID %in% last_id$bat]
			fillers$pitch[[length(fillers$pitch)]] <- fillers$pitch[[length(fillers$pitch)]][!(Name %in% lineup[[k]]$pitch)]
			fillers$pitch[[length(fillers$bat)]] <- unique(fillers$pitch[[length(fillers$pitch)]]$Name)		

		}

		fillers <- lapply(fillers, function(x){unique(unlist(x))})

		temp <- list()
		for(s in seasons){

			temp[[length(temp) + 1]] <- play_by_play[[s]][Batter_Name %in% fillers$bat & ID %in% last_id$bat]

		}


		names(temp) <- seasons
		temp <- dplyr::bind_rows(temp)


		matrices <- list()
		innings <- sort(unique(temp$Inn.))

		for(inn in innings){

			temp2 <- temp[Inn. == inn]
			Move_n <- sort(unique(temp2$Move_n))

			out <- list()

			for(move in Move_n){

				out[[length(out) + 1]] <- get_matrices(temp2[Move_n == move])

			}

			names(out) <- Move_n
			matrices[[length(matrices) + 1]] <- out

		}

		names(matrices) <- innings
		event_matrices_UA$bat <- matrices

		event_matrices_UA$bat[[length(event_matrices_UA$bat) + 1]] <- matrices

		#Info frame for weighting, ect.
		info <- unique(temp[, c("Inn.", "Move_n")])
		temp[, Count := lapply(.SD, length), by = c("Inn.", "Move_n"), .SDcol = "Move_n"]

		data.table::setkeyv(temp, c("Inn.", "Move_n"))
		data.table::setkeyv(info, c("Inn.", "Move_n"))

		info[temp, Count := i.Count]
		info[, N_Games := length(matches)]

		data.table::setorderv(info, c("Inn.", "Move_n"))

		event_matrices_UA$bat$n_events <- info



		temp <- list()
		for(s in seasons){

			temp[[length(temp) + 1]] <- play_by_play[[s]][Pitcher_Name %in% fillers$pitch & ID %in% last_id$pitch]

		}


		names(temp) <- seasons
		temp <- dplyr::bind_rows(temp)


		matrices <- list()
		innings <- sort(unique(temp$Inn.))

		for(inn in innings){

			temp2 <- temp[Inn. == inn]
			Move_n <- sort(unique(temp2$Move_n))

			out <- list()

			for(move in Move_n){

				out[[length(out) + 1]] <- get_matrices(temp2[Move_n == move])

			}

			names(out) <- Move_n
			matrices[[length(matrices) + 1]] <- out

		}

		names(matrices) <- innings
		event_matrices_UA$pitch <- matrices

		event_matrices_UA$pitch[[length(event_matrices_UA$pitch) + 1]] <- matrices

		#Info frame for weighting, ect.
		info <- unique(temp[, c("Inn.", "Move_n")])
		temp[, Count := lapply(.SD, length), by = c("Inn.", "Move_n"), .SDcol = "Move_n"]

		data.table::setkeyv(temp, c("Inn.", "Move_n"))
		data.table::setkeyv(info, c("Inn.", "Move_n"))

		info[temp, Count := i.Count]
		info[, N_Games := length(matches)]

		data.table::setorderv(info, c("Inn.", "Move_n"))

		event_matrices_UA$pitch$n_events <- info

		output[[k]] <- list(event_matrices = event_matrices,
								event_matrices_UA = event_matrices_UA)

	}

	names(output) <- c("home", "away")

	return(output)

}