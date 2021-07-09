# mlbDatabase

To install the package, simply do:

``` R

devtools::install_github("c-Stats/mlb_database")

``` 

Once this is done, you can clean the data extracted by the Python MLB webscrapper (see repo at: https://github.com/c-Stats/mlb_webscrapper). In the following section, 
the "path" argument is the same as the webscrapper's.

To update the score database, do:

``` R

mlbDatabase::update_database(path)

``` 

The play-by-play files, as well as the Markov chain modelling files, can be created and/or cleaned via:

``` R

mlbDatabase::process_PlayByPlay_data(path)
mlbDatabase::Setup_Markov_Database(path)

``` 

The data from LotoQc and Pinnacle can be cleaned via:


``` R

mlbDatabase::process_MiseOjeu_data(path)
mlbDatabase::process_Pinnacle_data(path)

``` 

Lastly, once the data has been cleaned and the two previous function ran, one can identify the current betting opportunities with positive expected returns, as well as evaluate the past 
performance of the optimal betting portfolio (in terms of mean expected geometrical returns). The final file will be avaible at: "path/MLB_Modeling/Betting/Predicted_Lineups/Arbitrage.rds"


``` R

mlbDatabase::arbitrage(path)

``` 

Note: the optimal weights for the portfolios are computed using a multivariate extend of the Kelly criterion, alongside a second-order series approximation. (No negative weights are allowed). The variance-covariance matrix
of the expected returns is estimated using historical data ranging from 2011 to 2020. (We do NOT consider betting outcomes to be independent, unless they each pertain to different matchups between different teams. Doing otherwise would be foolish.)
