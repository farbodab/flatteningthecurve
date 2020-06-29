## Computing a simple Rt -- not accounting for reporting delay/ testing
#Written by: Isha Berry

args = commandArgs(trailingOnly=TRUE)


##### Load packages
install.packages("EpiEstim",repos = "http://cran.us.r-project.org")
install.packages("incidence",repos = "http://cran.us.r-project.org")
install.packages("dplyr",repos = "http://cran.us.r-project.org")
install.packages("smoother",repos = "http://cran.us.r-project.org")
library(EpiEstim)
library(incidence)
library(magrittr)
library(dplyr)
library(smoother)



#Read in dataset
data<-read.csv(args[1],
               stringsAsFactors = FALSE)
#head(data)

#Start dataset on March 1 by filtering, data is more stable from this point onwards
covid_cases<- data %>%
  select(province, date_report, cumulative_cases) %>%
  mutate(date_report = as.Date(date_report, "%d-%m-%Y")) %>%
  rename(date = date_report) %>%
  rename(cases = cumulative_cases)%>%
  filter(date >= "2020-03-01") %>%
  filter(province == "Ontario")

#Create function to compute new cases (incidence) and smooth them -- Smoothing function
smooth_new_cases <- function(cases){
  cases %>%
    arrange(date) %>%
    mutate(new_cases = c(cases[1], diff(cases))) %>%
    mutate(new_cases_smooth = round(
      smoother::smth(new_cases, window = 7, tails = TRUE)
    )) %>%
    select(province, date, new_cases, new_cases_smooth)
}

#Applying smoothing function to dataset
province_selected <- "Ontario"
smooth<-covid_cases %>%
  filter(province == province_selected) %>%
  smooth_new_cases()

#plot incidence
#plot(as.incidence(smooth$new_cases_smooth, dates = smooth$date))


T <- nrow(covid_cases)
t_start <- seq(2, T-6) # starting at 2 as conditional on the past observations
t_end <- t_start + 6

#estimate R_t weektly
res_weekly <- estimate_R(smooth$new_cases_smooth,
                         method="parametric_si",
                         config = make_config(list(
                           t_start = t_start,
                           t_end = t_end,
                           mean_si = 3.96,
                           std_si = 4.75))
)

rt_daily<- res_weekly$R

write.csv(rt_daily,args[2], row.names = FALSE)
