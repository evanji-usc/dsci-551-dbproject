The raw_data folder contains the files for real world data. iris and cars are already loaded in as tables under the testdb database. However, linksmall and ratings_small need to be loaded in with the () command as they are quite large and the upload time in google drive was too extensive. 

The databases folder is created by the CLI implementation and stores the csv file for the databases. Each database will have its own csv file which contains the names of the tables it posseses as well as the datetime of which it was created.

As mentioned above the directories of car and iris are already loaded in and contain the chunked data.

The CLI_notebook contains the files cli.py, handle.py, and functions.py. Which are responsible for the implementation and handling of the command line interface commands.

Commands: 
help
show databases AND ! db testdb AND show tables AND ! table iris
> sepal.length 6 show sepal.width petal.length
() C:\Users\evanj\OneDrive\Documents\GitHub\dsci-551-dbproject\cars.csv cars
[] cyl
! table iris AND @ sepal.length
&& ratings_small linksmall movieId movieId save

Github link:
https://github.com/evanji-usc/dsci-551-dbproject.git