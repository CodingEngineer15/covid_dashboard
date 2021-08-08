import sqlalchemy
import pandas as pd
import sqlite3

#Read the entire database
entire_covid_df = pd.read_csv('data/owid-covid-data.csv')

covid_deaths_cols = ['iso_code', 'continent', 'location', 'date', 'total_cases', 'new_cases',
       'new_cases_smoothed', 'total_deaths', 'new_deaths',
       'new_deaths_smoothed', 'total_cases_per_million',
       'new_cases_per_million', 'new_cases_smoothed_per_million',
       'total_deaths_per_million', 'new_deaths_per_million',
       'new_deaths_smoothed_per_million','population']

covid_vaccinated_cols = ['iso_code', 'continent', 'location', 'date','tests_per_case',
       'new_tests', 'total_tests', 'total_tests_per_thousand', 'new_tests_per_thousand',
       'new_tests_smoothed', 'new_tests_smoothed_per_thousand',
       'positive_rate', 'tests_per_case', 'tests_units', 'total_vaccinations',
       'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations',
       'new_vaccinations_smoothed', 'total_vaccinations_per_hundred',
       'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
       'new_vaccinations_smoothed_per_million', 'stringency_index',
       'population']
    
       
deaths_df = entire_covid_df[covid_deaths_cols]
vaccine_df = entire_covid_df[covid_vaccinated_cols]

engine = sqlalchemy.create_engine('sqlite:///covid_db.sql')
deaths_df.to_sql('CovidDeaths', engine, index=False,if_exists='replace')
vaccine_df.to_sql('CovidVaccines',engine,index=False,if_exists='replace') 
conn = sqlite3.connect('covid_db.sql')

def query2csv(query,filename,conn=conn):
	"""Takes an sql database connection and performs the query and saves the output
	to a csv file of name 'filename'
	Input
	query: An sql query. Must be a string and compliant with sqlite3
	conn: An sql connection that must be compatible with pandas read_sql command
	filename: The name of the csv file which will be created from the resultant query to
	the sql database.
	
	Output
	df: Dataframe of the result of the sql query"""
	df = pd.read_sql(query,conn)
	df.to_csv(filename)
	return df

"""Skills used in sql queries:
* Joins
* Common table expressions
* Aggregrate functions
* Converting data types
"""

#Check out the total cases vs number of deaths
#Use cast to remove errors
total_deaths_and_cases_query = """
SELECT SUM(new_cases) AS total_cases, 
       SUM(cast(new_deaths AS INT)) AS total_deaths,
       SUM(cast(new_deaths AS INT))/SUM(New_Cases)*100 AS DeathPercentage
FROM CovidDeaths
WHERE continent IS NOT NULL 
ORDER BY 1,2
"""

#Determine total death count based on continent omitting Antarctica
regional_deaths_query = """
SELECT location, SUM(CAST(new_deaths AS INT)) AS TotalDeathCount
FROM CovidDeaths
WHERE continent IS NULL 
AND location NOT IN ('World', 'European Union', 'International')
GROUP BY location
ORDER BY TotalDeathCount DESC
"""

#Determine what percentage of the population is infected by covid
#Use MAX to get the up to date number of cases as the total number of cases will only increase  
perc_infected_query = """
SELECT Location, Population, MAX(total_cases) AS HighestInfectionCount,  MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM CovidDeaths
GROUP BY Location, Population
ORDER BY PercentPopulationInfected DESC
"""

#Shows the percentage of the population that has recieved at least one vaccination
rolling_vacs_query = """
WITH PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations AS INT)) OVER (PARTITION BY dea.Location Order BY dea.location, dea.Date) AS RollingPeopleVaccinated
FROM CovidDeaths dea
JOIN CovidVaccines vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
)
SELECT *, (RollingPeopleVaccinated/Population)*100 AS RollingVaccinatedPerc
FROM PopvsVac
"""

query2csv(total_deaths_and_cases_query,'data/totals.csv')
query2csv(regional_deaths_query,'data/regional_deaths.csv')
query2csv(perc_infected_query,'data/perc_infected.csv')
query2csv(total_deaths_and_cases_query,'data/vaccinated.csv')





