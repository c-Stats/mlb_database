spawn_vcov <- function(n){

	bound <- cbind(runif(n, 0, 1), rbinom(n = n, size = 1, p = 1/2))
	bound <- t(apply(bound, 1, sort))

	p <- apply(bound, 1, diff)
	sdevs <- sqrt(p * (1 - p))

	vcov_mat <- matrix(0, n, n)
	for(i in 1:n){

		for(j in i:n){

			a <- max(bound[i, 1], bound[j, 1])
			b <- min(bound[i, 2], bound[j, 2])

			p_intersect <- b - a
			max_val <- sdevs[i] * sdevs[j] + p[i] * p[j]

			if(abs(p_intersect) > max_val | p_intersect < 0){

				next

			} else {

				vcov_mat[i,j] <- p_intersect

			}


		}

	}

	vcov_mat[lower.tri(vcov_mat)] <- t(vcov_mat)[lower.tri(vcov_mat)]
	vcov_mat <- vcov_mat - p %*% t(p)
	diag(vcov_mat) <- p * (1 - p)

	scaling_factors <- sqrt(p * (1 - p))
	vcov_mat <- vcov_mat / scaling_factors %*% t(scaling_factors)

	return(vcov_mat)

}



#Simulates I, p_P and b_Q 
simulate_correlated_bernoulli <- function(chol_mat, chol_PQ, margin_bounds){

	z <- rnorm(nrow(chol_mat))
	epsilon <- t(t(chol_PQ) %*% rbind(rnorm(nrow(chol_mat)), rnorm(nrow(chol_mat))))
	epsilon <- apply(epsilon, 2, function(x){x + z})

	z <- pnorm(t(chol_mat) %*% z)
	epsilon <- pnorm(t(chol_mat) %*% epsilon)

	p <- list(z = z,
				P = epsilon[, 1],
				Q = epsilon[, 2])


	I <- sapply(p$z, function(x){rbinom(n = 1, size = 1, p = x)})
	p <- lapply(p, function(x){c(x, 1- x)})
	I <- c(I, 1 - I)

	b <- 1 / p$Q - runif(n = length(I), min = margin_bounds[1], max = margin_bounds[2])
	b <- sapply(b, function(x){min(x, 10)})

	return(list(I = I,
				p = p$z,
				p_P = p$P,
				p_Q = p$Q,
				b_Q = b))

}



simulate_arbitrage <- function(chol_mat, chol_PQ, margin_bounds, covar_p){

	simulation <- simulate_correlated_bernoulli(chol_mat, chol_PQ, margin_bounds)

	logloss <- c(sum(simulation$I * log(simulation$p_P)),
					sum(simulation$I * log(simulation$p_Q)),
					sum(simulation$I * log(1 / simulation$b_Q)))

	logloss <- - logloss / nrow(chol_mat)
	names(logloss) <- c("P", "Q", "b_Q")

	mu <- simulation$p_P * simulation$b_Q - 1
	keep <- which(mu > 0)

	if(length(keep) < 2){

		return(NULL)

	}

	simulation <- lapply(simulation, function(x){x[keep]})

	vcov_mat <- covar_p[keep, keep]

	diag(vcov_mat) <- simulation$p_P * (1 - simulation$p_P)
	vcov_mat <- vcov_mat * (simulation$b_Q %*% t(simulation$b_Q))

	#add safe asset
	mu <- c(0, mu[keep])
	vcov_mat <- rbind(rep(0, ncol(vcov_mat)), vcov_mat)
	vcov_mat <- cbind(rep(0, nrow(vcov_mat)), vcov_mat)

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


	optim_sol <- alabama::auglag(par = rep(1, length(mu)) / length(mu),
								fn = f_to_min,
								gr = g_f_to_min,
								heq = heq,
								heq.jac = heq.jac,
								hin = hin,
								hin.jac = hin.jac,
								control.outer = list(method="nlminb"))

	weights <- sapply(optim_sol$par, function(x){max(0, x)})
	weights <- weights / sum(weights)

	out <- list(simulation = simulation,
					geom_r = as.numeric(weights[-1] %*% (simulation$I * simulation$b_Q - 1)),
					logloss = logloss,
					optim_val = (1 - optim_sol$value)^(length(keep)) - 1)

	return(out)

}


simulate_path <- function(capital, npath, chol_mat, chol_PQ, margin_bounds, covar_p){

	t <- 0
	log_loss <- c(0,0,0)
	names(log_loss) <- c("P", "Q", "b_Q")

	path <- rep(0, npath)
	optim_val <- rep(0, npath)
	initial_capital <- capital

	while(capital > 1 & t < npath & capital/initial_capital <= 10){

		arb <- simulate_arbitrage(chol_mat, chol_PQ, margin_bounds, covar_p)
		if(is.null(arb)){

			next

		}

		capital <- capital * (1 + arb$geom_r)
		log_loss <- log_loss + arb$logloss

		t <- t + 1
		path[t] <- capital
		optim_val[t] <- arb$optim_val

	}

	log_loss <- log_loss / npath

	no_margin_geom_r <- -diff(log_loss)
	no_margin_E_capital <- capital * (1 + no_margin_geom_r)^npath

	geom_r <- (capital[length(capital)] / initial_capital)^(1/npath) - 1

	return(list(path = c(initial_capital, path),
					geom_r = geom_r,
					cross_entropy = log_loss[1],
					cross_entropy_diff = no_margin_geom_r,
					optim_val = prod(1 + optim_val)^(1 / npath) - 1))


}


vcov_mat <- spawn_vcov(50) 
chol_mat <- chol(vcov_mat)

cor_PQ <- matrix(c(1, 0.99, 0.99, 1), 2, 2)
sd_PQ <- sqrt(c(0.2, 0.2))

var_PQ <- cor_PQ * (sd_PQ %*% t(sd_PQ))
chol_PQ <- chol(var_PQ)

margin_bounds <- c(0.03, 0.07)

simulated_sample <- t(replicate(20000, simulate_correlated_bernoulli(chol_mat, chol_PQ, margin_bounds)$I))
covar_p <- var(simulated_sample)


test <- simulate_path(capital = 10^4, npath = 100, chol_mat, chol_PQ, margin_bounds, covar_p)