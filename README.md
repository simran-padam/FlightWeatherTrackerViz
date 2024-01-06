![Alt text](airplane.jpg)

# Flight Tracker with Weather Risk Assessment

Submitted by: Shriniket Buche (ssb2215), Simran Padam (sdp2158), Harsh Benahalkar (hb2776)

This is the code repository for the EECS 6893: Big Data Analysis
final project Flight Data Analytics. This project includes 
two applications, a future flight planner and an individual
flight tracker.

Flies Included:
- `flight_tracker_application.py` : main python script to run the app
- `environment.yml` : YAML file to define python environment and packages
- `README.md` : README file
- `assets/style.css` : CSS file for styling dropdowns in the app

### Project Setup

Please have conda installed on your system. In the current directory please run

`conda env create -f environment.yml`

This will create a new conda environment called flight-tracker. Please activate this environment using

`conda activate flight-tracker`

After which you can run the app using 

`python flight_tracker_application.py`

If there is trouble in this setup, you can access `environment.yml` directly to see which packages should be installed,
the python version is 3.9.17.

### Application Usage
There are two applications each in its own tab located at the top. To access each application, toggle the respective tab.
For the future flight planner, allow up to 10 minutes from startup to see the final plot displayed on your screen. There
are background API fetching processes that can take some time.

For the individual flight tracker, the first dropdown is used to select an airport, you can search for your respective airport
or select it directly from the dropdown. After at most 1 minute, a table and a second dropdown will be populated. The table
will show the arrivals to your chosen airport and the second dropdown will have the respective flights. Upon selecting a flight, 
a map will appear at the bottom showing the progress of the flight.
