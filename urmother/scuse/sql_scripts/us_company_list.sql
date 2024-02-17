
drop table temp_ticker_sic_1;
create temporary table temp_ticker_sic_1 as
select
	 cast( w1.cik as int ) as "cik"
	, cast( w1.sic as int ) as "sic"
	, cast( w1.fiscal_year_end as int ) as "fiscal_year_end"
from
	ticker_sic w1
;

drop table temp_sic_code_1;
create temporary table temp_sic_code_1 as
select
	 cast( w1.sic_code as int ) as "sic_code"
	, w1.office
	, w1.industry_title
from
	sic_code w1
;

drop table temp_ticker_names_1;
create temporary table temp_ticker_names_1 as
select
	 cast( w1.cik as int ) as "cik"
	, upper( w1.name ) as "company_name"
	, w1.ticker as "ticker_name"
	, upper( w1.exchange ) as "exchange"
from
	company_tickers_exchange w1
;



--GENERATING FULL COMPANY LIST
drop table full_company_list;
create temporary table full_company_list as
select
	 w3.company_name
	, w1.cik
	, w3.ticker_name
	, w3.exchange
	, w1.sic as "sic_code"
	, w2.office as "sic_office"
	, w2.industry_title
from
	temp_ticker_sic_1		w1
left join
	temp_sic_code_1			w2 on
		 w1.sic = w2.sic_code
left join
	temp_ticker_names_1		w3 on
		 w1.cik = w3.cik
where
	 w2.office is not null
;


--SUMMARY
select
	 w1.sic_office
	, count( distinct w1.sic_code ) as "count_sic"
	, count( w1.company_name ) as "count_companies"
from
	full_company_list w1
group by
	 w1.sic_office
order by
	 count( w1.company_name ) desc
;


--REVIEW SELECTED LIST
select
	 w1.company_name
	, w1.cik as "central_index_key"
	, group_concat( distinct w1.ticker_name ) as "ticker_name"
	, group_concat( distinct w1.exchange ) as "exchange"
	, w1.sic_code
	, w1.sic_office
	, w1.industry_title
from
	full_company_list w1
where
--	 lower( w1.sic_office ) in(
--	 	 'office of manufacturing'
----	 	, 'office of energy & transportation'
--	 )
--and
	 lower( w1.industry_title ) like '%air transportation, scheduled%'	--'%motors & generators%'
and(
	 lower( w1.exchange ) not in( 'otc' )
	 	and
	 lower( w1.exchange ) is not null
)
and
	 lower( w1.company_name ) in(
	 	 "delta air lines, inc."
	 	, "united airlines holdings, inc."
	 	, "american airlines group inc."
--	 	, "southwest airlines co"
	 )
group by
	 1,2,5,6,7
order by
	 w1.sic_office
	, w1.industry_title
	, w1.company_name
;



--ADHOC LIST
select
	 w1.*
from
	full_company_list w1
where
	 lower( w1.company_name ) like '%occidental%'
;



--ADHOC LIST
select
	 w1.sic_office
	, count( distinct w1.company_name ) as "count_companies"
from
	full_company_list w1
group by
	 w1.sic_office
;



--ADHOC LIST
select
	 w1.sic_office
	, w1.industry_title
	, count( distinct w1.company_name ) as "count_companies"
from
	full_company_list w1
group by
	 w1.sic_office
	, w1.industry_title
;


--=================================
--	DROP TEMP TABLES
--=================================

drop table temp_ticker_sic_1;
drop table temp_sic_code_1;
drop table temp_ticker_names_1;
drop table full_company_list;
	 


