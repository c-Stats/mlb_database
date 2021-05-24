#' Process play-by-play data
#'
#' This function creates the database needed to set up Markov chains.
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'

Setup_Markov_Database <- function(path){

	pbp_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Play_by_play_Database.rds", sep = "")
	if(file.exists(pbp_path)){

		play_by_play <- readRDS(pbp_path)

	} else {

		print("File missing at:", quote = FALSE)
		print(pbp_path, quote = FALSE)

	}



	#Extract state transitions
	print("Processing play-by-play files...", quote = FALSE)
	pb <- txtProgressBar(min = 0, max = length(play_by_play), style = 3)
	for(i in 1:length(play_by_play)){

		#At bat/pitch scores
		play_by_play[[i]][Team_Home == Team_Pitch, Score_Pitch := Score_Home]
		play_by_play[[i]][Team_Away == Team_Pitch, Score_Pitch := Score_Away]

		play_by_play[[i]][Team_Home == Team_Bat, Score_Bat := Score_Home]
		play_by_play[[i]][Team_Away == Team_Bat, Score_Bat := Score_Away]	

		#Points score for at bat
		play_by_play[[i]][, Points_scored_bat := lapply(.SD, function(x){diff(c(0, x))}), by = c("ID", "Team_Bat"), .SDcol = "Score_Bat"]

		#Tag move numbers
		play_by_play[[i]][, Move_n := lapply(.SD, function(x){c(1:length(x))}), by = c("ID"), .SDcol = "Inn."]
		play_by_play[[i]][, Is_final := lapply(.SD, function(x){

															if(length(x) == 1){TRUE} else {c(diff(x) != 1, TRUE)}

															}),
							by = c("ID", "Batter_Name"),
							.SDcol = "Move_n"]


		#Tag move numbers
		play_by_play[[i]][, Move_n := lapply(.SD, function(x){

															if(length(x) == 1){

																c(1)

															} else {

																d <- diff(x)
																u <- 1
																out <- rep(0, length(x))
																for(j in 1:length(d)){

																	out[j] <- u
																	if(d[j] != 1){u <- u + 1}

																}

																out[length(out)] <- u
																return(out)

															}

															}), by = c("ID", "Inn.", "Batter_Name"), .SDcol = "Move_n"]	



		#Add next state
		play_by_play[[i]][, Next_base := lapply(.SD, function(x){

																	if(length(x) > 1){

																		c(x[2:length(x)], "End")

																	} else {

																		c("End")

																	}
																	
																}), 

							by = c("ID", "Team_Bat", "Inn."), .SDcol = "Base"]


		play_by_play[[i]][, Next_out := lapply(.SD, function(x){

																	if(length(x) > 1){

																		c(x[2:length(x)], 3)

																	} else {

																		c(3)

																	}
																	
																}), 

							by = c("ID", "Team_Bat", "Inn."), .SDcol = "Outs"]


		#Add previous state, previous outs and points scored, accounting for back-to-back moves
		play_by_play[[i]][, Previous_base := lapply(.SD, function(x){

																	x[length(x)] <- x[1]
																	x
																	
																}), 

							by = c("ID", "Batter_Name", "Inn.", "Move_n"), .SDcol = "Base"]	


		play_by_play[[i]][, Previous_out := lapply(.SD, function(x){

																	x[length(x)] <- x[1]
																	x
																	
																}), 

							by = c("ID", "Batter_Name", "Inn.", "Move_n"), .SDcol = "Outs"]	


		play_by_play[[i]][, Points_scored_bat := lapply(.SD, sum), by = c("ID", "Team_Bat", "Batter_Name", "Inn.", "Move_n"), .SDcol = "Points_scored_bat"]

		play_by_play[[i]][, Play := lapply(.SD, function(x){

																if(length(x) == 1){

																	x

																} else {

																	rep(paste(x, collapse = " + "), length(x))

																}
		

															}), 
			by = c("ID", "Inn.", "Batter_Name", "Move_n"),
			.SDcol = "Play"]



		play_by_play[[i]] <- play_by_play[[i]][Is_final == TRUE]

		play_by_play[[i]][, Is_bat_event := FALSE]
		play_by_play[[i]][Move_n > 1 & Points_scored_bat > 1, Is_bat_event := TRUE]
		play_by_play[[i]][Move_n == 1, Is_bat_event := TRUE]

		setTxtProgressBar(pb, i)

	}




	################################################
	########### Load database       ################
	################################################

	load_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds", sep = "")
	DB <- readRDS(load_path)


	load_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds", sep = "")
	scores <- readRDS(load_path)

	data.table::setkey(scores, "ID")


	#Add implied winning probabilities
	for(i in 1:length(play_by_play)){

		data.table::setkey(play_by_play[[i]], "ID")
		play_by_play[[i]][scores, Factor_home := i.Factor_Home_Historical]
		play_by_play[[i]][scores, Factor_away := i.Factor_Away_Historical]
		play_by_play[[i]][scores, P_Home_Win := i.P_Home_Win_Historical]

	}



	unique_transitions <- dplyr::bind_rows(play_by_play)

	unique_transitions[, P_points := lapply(.SD, function(x){

												u <- unique(x)
												n <- sapply(u, function(i){length(which(x == i))})
												return(n[match(x, u)] / length(x))


						}),
						by = c("Inn.", "Previous_base", "Previous_out", "Next_base", "Next_out",
																	"Move_n"),
						.SDcol = "Points_scored_bat"]

	unique_transitions[, Points := Points_scored_bat]	
	unique_transitions[, Points_scored_bat := NULL]

	unique_transitions[, Avg_Points := lapply(.SD, mean), by = c("Inn.", "Previous_base", "Previous_out", "Next_base", "Next_out",
																	"Move_n"),
															.SDcol = "Points"]

	unique_transitions <- unique(unique_transitions[, c("Inn.", "Previous_base", "Previous_out", "Next_base", "Next_out",
																	"Move_n", "Points", "P_points", "Avg_Points"), with = FALSE])

	data.table::setorderv(unique_transitions, c("Previous_out", "Next_out", "Avg_Points"))


	states <- unique(unique_transitions[, c("Previous_base", "Previous_out", "Next_base", "Next_out", "Avg_Points"), with = FALSE])
	states2 <- list(from = states[, c("Previous_base", "Previous_out", "Avg_Points"), with = FALSE],
					to = states[, c("Next_base", "Next_out", "Avg_Points"), with = FALSE])
	for(i in 1:2){names(states2[[i]])[1:2] <- c("Base", "Outs")}

	states <- dplyr::bind_rows(states2)
	states2 <- NULL
	states <- unique(states[, c("Base", "Outs"), with = FALSE])

	right_order <- c("___", "1__", "_2_", "__3", "12_", "1_3", "_23", "123", "End")
	gr <- as.data.table(expand.grid(x = right_order, y = c(0, 1, 2, 3)))
	names(gr) <- names(states)[1:2]
	gr[, i := c(1:nrow(gr))]

	data.table::setkeyv(states, names(states)[1:2])
	data.table::setkeyv(gr, names(states)[1:2])

	states <- gr[states]
	data.table::setorder(states, i)
	states[, i := c(1:nrow(states))]

	states_DB <- list(states_i = states[, names(states), with = FALSE],
						states_j = states[, names(states), with = FALSE])

	names(states_DB$states_i)[1:2] <- c("Previous_base", "Previous_out")
	names(states_DB$states_j) <- c("Next_base", "Next_out", "j")

	data.table::setkeyv(states_DB$states_i, c("Previous_base", "Previous_out"))
	data.table::setkeyv(states_DB$states_j, c("Next_base", "Next_out"))

	for(i in 1:length(play_by_play)){

		data.table::setkeyv(play_by_play[[i]], c("Previous_base", "Previous_out"))
		play_by_play[[i]][states_DB$states_i, i := i.i]

		data.table::setkeyv(play_by_play[[i]], c("Next_base", "Next_out"))
		play_by_play[[i]][states_DB$states_j, j := i.j]

		play_by_play[[i]][, Batting_Order := lapply(.SD, cumsum), by = c("ID", "Inn.", "Team_Bat"), .SDcol = "Is_bat_event"]
		play_by_play[[i]][Is_bat_event == FALSE, Batting_Order := NA]

		play_by_play[[i]][Is_bat_event == TRUE, Batting_Order := lapply(.SD, function(x){

																					x[] <- max(x)
																					return(x)

																						}),
												by = c("ID", "Inn.", "Team_Bat", "Batter_Name"),
												.SDcol = "Batting_Order"]

	save_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Markov_Database.rds", sep = "")
	saveRDS(play_by_play, save_path)											

	}

}








