
--covide_death_database

SELECT *
from SQL_PROJECT..COVID_DEATH$ 
where continent is not null
order by 3 ,4;

--select * from SQL_PROJECT..COVID_VACCINATION$
--order by 3,4

--select the data that we are going to work on  

select location,date,population,total_cases,new_cases,total_deaths from SQL_PROJECT..COVID_DEATH$
where continent is not null
order by 1,2

--looking total cases vs total deaths
--show likelihood of death of someone comes in connect of someone 


select location, date, total_cases, total_deaths,(total_deaths/total_cases)*100 as percentage_death 
from SQL_PROJECT..COVID_DEATH$
where continent is not null
--where location like '%State%'
order by 1,2 desc



--looking at the total cases vs popluation shows what percentage of population got covid

select location, date, total_cases, population, (total_cases/population)*100 as percentpopluationinfected
from SQL_PROJECT..COVID_DEATH$
where continent is not null
--where location like '%State%'
order by 1,2

--looking at the countries with highest infection rate

select location, population, max(total_cases) as highest_infectionrate, max((total_cases/population))*100 as percentpopluationinfected
from SQL_PROJECT..COVID_DEATH$
where continent is not null
group by location, population
order by percentpopluationinfected desc


--showing countries highest death count per population

select location, max(cast(total_deaths as int)) as totaldeathcount 
from SQL_PROJECT..COVID_DEATH$ 
where continent is not null
group by location 
order by totaldeathcount desc


--lets break the things down by continents in terms of highest death count per population
select * from SQL_PROJECT..COVID_DEATH$

select continent, max(cast(total_deaths as int)) as totaldeathcount from SQL_PROJECT..COVID_DEATH$
where continent in ('North America','Asia','Africa','Oceania','South America','Europe') and continent is not null
group by continent
order by totaldeathcount desc


--showing the continent looking total cases vs total deaths
--show likelihood of death of someone comes in connect of someone infected

select continent, date, total_cases, total_deaths,(total_deaths/total_cases)*100 as percentage_death 
from SQL_PROJECT..COVID_DEATH$
where continent is not null
--where location like '%State%'
order by 1,2


--looking at the total cases vs popluation shows what percentage of population got covid in a continent
select continent, date, total_cases, population, (total_cases/population)*100 as percentpopluationinfected
from SQL_PROJECT..COVID_DEATH$
where continent is not null
--where location like '%State%'
order by 1,2 desc

---looking at the countries with highest infection rate

select continent, sum(population), sum(total_cases) as highest_infectionrate, max((total_cases/population))*100 as percentpopluationinfected
from SQL_PROJECT..COVID_DEATH$
--where continent is not null
group by continent
order by percentpopluationinfected desc


--showing countries highest death count per population

select continent, max(cast(total_deaths as int)) as totaldeathcount 
from SQL_PROJECT..COVID_DEATH$ 
where continent is not null
group by continent 
order by totaldeathcount desc


--lets break the things down by continents in terms of highest death count per population
select * from SQL_PROJECT..COVID_DEATH$

select continent, max(cast(total_deaths as int)) as totaldeathcount from SQL_PROJECT..COVID_DEATH$
where continent in ('North America','Asia','Africa','Oceania','South America','Europe') and continent is not null
group by continent
order by totaldeathcount desc


--GLOBAL_NUMBER 

--select date, sum(new_cases) as total_cases,sum(cast(new_deaths as int)) 
select  sum(new_cases) as total_cases,sum(cast(new_deaths as int))
new_death,sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage 
from SQL_PROJECT..COVID_DEATH$
where continent is not null
--group by date
order by 1,2


--covid_vaccination_data

--looking at the total vaccination vs total population

select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
from SQL_PROJECT..COVID_DEATH$ dea
join
SQL_PROJECT..COVID_VACCINATION$ vac
on dea.location=vac.location and
 dea.date = vac.date
 where dea.continent is not null
 order by 2,3

 --create view for looking at the total vaccination vs total population
 create view popvsvacc1 as
 select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
from SQL_PROJECT..COVID_DEATH$ dea
join
SQL_PROJECT..COVID_VACCINATION$ vac
on dea.location=vac.location and
 dea.date = vac.date
 where dea.continent is not null
 --order by 2,3
 select * from popvsvacc

 --looking at the total vaccination vs total population

 select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3

--create view for looking at the total vaccination vs total population
create view popvsvacci as
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3



--use CTE
with popvsvac(continent, location, date, population, vaccination, RollingPeopleVaccinated)
as 
(select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3)
)
select * , (RollingPeopleVaccinated/population)*100 as popvacc from popvsvac

--Temp Table
drop table if exists #percentpopulationvaccinated
create table #percentpopulationvaccinated( 
continent nvarchar(255),
location nvarchar(255),
Date datetime,
Population numeric,
New_Vaccinations numeric,
RollingPeopleVaccinated numeric
)

insert into #percentpopulationvaccinated
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3)

select * ,(RollingPeopleVaccinated/population)*100 as #percentpopulationvaccinated from #percentpopulationvaccinated


--creating view to store data for leter visualalization


Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 


drop view if exists PopulationVaccinated
Create View PopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From SQL_PROJECT..COVID_DEATH$ dea
Join SQL_PROJECT..COVID_VACCINATION$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 

select * from PopulationVaccinated ;