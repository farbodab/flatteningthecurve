## Computing a simple Rt -- not accounting for reporting delay/ testing
##### Load packages
library(EpiEstim)
library(incidence)
library(magrittr)
library(dplyr)
library(httr) # downloading from URLs
library(readxl) # read xlsx
​
#READ IN DATASET
data <- read.csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv",
                 stringsAsFactors = FALSE)
#head(data)
​
​
#SET UP PARAMETERS -- assumptions based on literature
#https://wwwnc.cdc.gov/eid/article/26/6/20-0357_article
# We use parameter estimates from this article
si<-3.96    #mean serial interval
sd<-4.75    #standard deviation
start_date<- "2020-03-25"
rt_start_date<-"2020-04-01"
​
#CLEAN UP DATASET
#Start dataset on March 30 by filtering, cases are more stable
covid_cases<- data %>%
  select(province, date_report, cases) %>%
  mutate(date_report = as.Date(date_report, "%d-%m-%Y")) %>%
  rename(date = date_report) %>%
  filter(date >= start_date)
​
​
#Alberta
ab_cases<-covid_cases %>%
  filter(province=="Alberta")
​
#estimate R_t weektly
res_weekly_ab <- estimate_R(ab_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
#clean up dataset
res_weekly_ab <- res_weekly_ab%>%
  select(t_start, t_end, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_ab = `Mean(R)`) %>%
  rename(rt_low_ab = `Quantile.0.025(R)`)%>%
  rename(rt_up_ab = `Quantile.0.975(R)`)
​
​
#BC
bc_cases<-covid_cases %>%
  filter(province=="BC")
​
#estimate R_t weektly
res_weekly_bc <- estimate_R(bc_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_bc <- res_weekly_bc%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_bc = `Mean(R)`) %>%
  rename(rt_low_bc = `Quantile.0.025(R)`)%>%
  rename(rt_up_bc = `Quantile.0.975(R)`)
​
​
#Manitoba
mb_cases<-covid_cases %>%
  filter(province=="Manitoba")
​
#estimate R_t weektly
res_weekly_mb <- estimate_R(mb_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_mb <- res_weekly_mb%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_mb = `Mean(R)`) %>%
  rename(rt_low_mb = `Quantile.0.025(R)`)%>%
  rename(rt_up_mb = `Quantile.0.975(R)`)
​
#New Brunswick
nb_cases<-covid_cases %>%
  filter(province=="New Brunswick")
​
#estimate R_t weektly
res_weekly_nb <- estimate_R(nb_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_nb <- res_weekly_nb%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_nb = `Mean(R)`) %>%
  rename(rt_low_nb = `Quantile.0.025(R)`)%>%
  rename(rt_up_nb = `Quantile.0.975(R)`)
​
#Newfoundland
nl_cases<-covid_cases %>%
  filter(province=="NL")
​
#estimate R_t weektly
res_weekly_nl <- estimate_R(nl_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_nl <- res_weekly_nl%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_nl = `Mean(R)`) %>%
  rename(rt_low_nl = `Quantile.0.025(R)`)%>%
  rename(rt_up_nl = `Quantile.0.975(R)`)
​
#Nova Scotia
ns_cases<-covid_cases %>%
  filter(province=="Nova Scotia")
​
#estimate R_t weektly
res_weekly_ns <- estimate_R(ns_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_ns <- res_weekly_ns%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_ns = `Mean(R)`) %>%
  rename(rt_low_ns = `Quantile.0.025(R)`)%>%
  rename(rt_up_ns = `Quantile.0.975(R)`)
​
#Ontario
on_cases<-covid_cases %>%
  filter(province=="Ontario")
​
#estimate R_t weektly
res_weekly_on <- estimate_R(on_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_on <- res_weekly_on%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_on = `Mean(R)`) %>%
  rename(rt_low_on = `Quantile.0.025(R)`)%>%
  rename(rt_up_on = `Quantile.0.975(R)`)
​
#Quebec
qc_cases<-covid_cases %>%
  filter(province=="Quebec")
​
#estimate R_t weektly
res_weekly_qc <- estimate_R(qc_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_qc <- res_weekly_qc%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_qc = `Mean(R)`) %>%
  rename(rt_low_qc = `Quantile.0.025(R)`)%>%
  rename(rt_up_qc = `Quantile.0.975(R)`)
​
#Saskwatchewan
sk_cases<-covid_cases %>%
  filter(province=="Saskatchewan")
​
#estimate R_t weektly
res_weekly_sk <- estimate_R(sk_cases$cases,
             method="parametric_si",
             config = make_config(list(
               mean_si = si,
               std_si = sd)))$R
​
res_weekly_sk <- res_weekly_sk%>%
  select(t_start, `Mean(R)`, `Quantile.0.025(R)`,`Quantile.0.975(R)` )%>%
  rename(rt_sk = `Mean(R)`) %>%
  rename(rt_low_sk = `Quantile.0.025(R)`)%>%
  rename(rt_up_sk = `Quantile.0.975(R)`)
​
​
#Merge the datasets into 1
combined<-merge(res_weekly_ab,res_weekly_bc, by ="t_start")
combined<-merge(combined, res_weekly_mb, by='t_start')
combined<-merge(combined, res_weekly_nb, by='t_start')
combined<-merge(combined, res_weekly_nl, by='t_start')
combined<-merge(combined, res_weekly_ns, by='t_start')
combined<-merge(combined, res_weekly_on, by='t_start')
combined<-merge(combined, res_weekly_qc, by='t_start')
combined<-merge(combined, res_weekly_sk, by='t_start')
​
#Add date variable
##note that the first date rt is for day 8 (i.e. April 1)
date_rep<-covid_cases %>%
  filter(date >= rt_start_date) %>%
  filter(province == "Ontario") %>%
  select(date)
date_rep$t_start<-res_weekly_ab$t_start
combined<-merge(combined, date_rep, by='t_start')
​
#Clean up the dataset
combined<-combined %>%
  mutate(date_report = date) %>%
  select(date_report, everything()) %>%
  select(-t_start, -t_end, -date)
​
#write to csv
write.csv(combined, "combined_rt.csv", row.names = FALSE)
