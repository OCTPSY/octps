
select
	 e1.*
from(
		select
			 w1.type
			, cast( replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as int ) as 'filing_number'
			, w1.name
		from
			sqlite_master w1
		where
			 w1.type = 'table'
) e1
where
	 e1.filing_number = 10548017627969
and(
	 lower( e1.name ) like '%depreciation%'
	 	or
	 lower( e1.name ) like '%consolidated%'
)
;


drop table temp_report_name;
create temporary table temp_report_name as
select
	 "10_K2022_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921022671322" as "report_name"
;


drop table temp_report_name_1;
create temporary table temp_report_name_1 as
select
	 report_name
	, replace( report_name, rtrim( report_name, replace( report_name, '_', '' ) ), '' ) as "filing_number"
from
	temp_report_name
;


select
	 w2.company_name
	, w1.*
from
	individual_report_links w1
left join
	filing_list w2 on
		 w1.filing_number = w2.filing_number
where
	 w1.filing_number = ( select filing_number from temp_report_name_1 )
--and
--	 lower( w2.company_name ) like '%san filippo%'
--and
--	 lower( w1.short_name ) like '%property%'
order by
	 w1.short_name
;


select
	 w1.*
from
	sqlite_master w1
where
	 lower( w1.name ) like '%' || ( select filing_number from temp_report_name_1 ) || '%'





--======================
--	DROP TEMP TABLES
--======================

drop table temp_report_name;
drop table temp_report_name_1;
