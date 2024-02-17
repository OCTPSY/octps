--============================================
--	COMPANY NAME : WHOLE EARTH BRANDS, INC.	--UPDATE HERE
--	CENTRAL INDEX KEY : 1753706				--UPDATE HERE
--============================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '1753706' as "cik"		--UPDATE HERE
	, '%balance_sheets_%' as "bs_include"
	, '%balance_sheets_parenthetical%' as "bs_exclude"
	, '%statements_of_earnings_%' as "is_include"
;


drop table temp_sqlitemaster;
create temporary table temp_sqlitemaster as
select
	 type
	, upper( --name
	  	 replace(
	  	 	 lower( name )
	  	 	, 'operations'
	  	 	, 'earnings' 
	  	 )
	  ) as "name"
from
	sqlite_master
;


--TEMPLATE TO UNION FOR ALL BALANCE SHEET SUMMARY
select
	 'select * from "' || w1.name || '"' as "bs_query"
--	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "bs_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, group_concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
from
	 temp_sqlitemaster 	w1
left join
	filing_list			w2 on
		 replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' )
		 =
		 w2.filing_number
where
	 w1.type = 'table'
and
	 w1.name like '10_K%'
and
	 w2.cik = ( select cik from temp_cik )
and
	 lower( w1.name ) like ( select bs_include from temp_cik )
and
	 lower( w1.name ) not like ( select bs_exclude from temp_cik )
--group by
--	 1,2,3,4
;



--TEMPLATE TO UNION FOR ALL INCOME STATEMENT SUMMARY

--select * from "10_K2017_02_10_Consolidated_Statements_Of_Operations_2251317588807"

select
	 'select * from "' || w1.name || '"' as "is_query"
--	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "is_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, group_concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
from
	 temp_sqlitemaster 	w1
left join
	filing_list			w2 on
		 replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' )
		 =
		 w2.filing_number
where
	 w1.type = 'table'
and
	 w1.name like '10_K%'
and
	 w2.cik = ( select cik from temp_cik )
and
	 lower( w1.name ) like ( select is_include from temp_cik )
--and
--	 lower( w1.name ) not like ( select exclude from temp_cik )
--group by
--	 1,2,3,4
;


--TO VIEW COMPILED BALANCE SHEET
drop table temp_balance_sheet;
create temporary table temp_balance_sheet as
select
	 ( select cik from temp_cik ) as "cik"
	, e1.*
from(
	--INSERT QUERY HERE
	select
		 w1.date
		, w1.cash_and_cash_equivalents / 1000 as "cash_and_equivalents"
		, w1.accounts_receivable_net_of_allowances_of_1285_and_955_respectively / 1000 as "accounts_receivable_net"
		, w1.inventories / 1000 as "inventories"
		, w1.total_current_assets / 1000 as "total_current_assets"
		, ( coalesce( w1.property_plant_and_equipment_net, 0 )
		  + coalesce( w1.operating_lease_right_of_use_assets, 0 ) ) / 1000
		  as "property_plant_and_equipment_net"
		, w1.goodwill / 1000 as "goodwill"
		, w1.other_intangible_assets_net / 1000 as "intangible_assets"
		, w1.total_assets / 1000 as "total_assets"
		, ( coalesce( w1.current_portion_of_operating_lease_liabilities, 0 )
		  + coalesce( w1.current_portion_of_long_term_debt, 0 ) ) / 1000
		  as "short_term_debt"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, ( coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.operating_lease_liabilities_less_current_portion, 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( w1.current_portion_of_operating_lease_liabilities, 0 )
		  + coalesce( w1.current_portion_of_long_term_debt, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.operating_lease_liabilities_less_current_portion, 0 ) ) / 1000
		  as "total_debt"
		, w1.total_liabilities / 1000 as "total_liabilities"
		, ( coalesce( w1.common_stock_0_0001_par_value_220000000_shares_authorized_38871646_and_38426669_shares_issued_and_outstanding_at_december_31_2021_and_december_31_2020_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000
		  as "total_paid_in_capital"
		, null / 1000 as "treasury_stock"
		, w1.accumulated_deficit / 1000 as "retained_earnings"
		, w1.total_stockholders_equity as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity as "total_liabilities_and_equity"
	from
		"10_K2022_03_14_CONSOLIDATED_BALANCE_SHEETS_13888022737458" w1
		union all
	select
		 w1.date
		, w1.cash_and_cash_equivalents / 1000 as "cash_and_equivalents"
		, w1.accounts_receivable_net_of_allowances_of_955_and_2832_respectively / 1000 as "accounts_receivable_net"
		, w1.inventories / 1000 as "inventories"
		, w1.total_current_assets / 1000 as "total_current_assets"
		, ( coalesce( w1.property_plant_and_equipment_net, 0 )
		  + coalesce( w1.operating_lease_right_of_use_assets, 0 ) ) / 1000
		  as "property_plant_and_equipment_net"
		, w1.goodwill / 1000 as "goodwill"
		, w1.other_intangible_assets_net / 1000 as "intangible_assets"
		, w1.total_assets / 1000 as "total_assets"
		, ( coalesce( w1.current_portion_of_operating_lease_liabilities, 0 )
		  + coalesce( w1.current_portion_of_long_term_debt, 0 ) ) / 1000
		  as "short_term_debt"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, ( coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.operating_lease_liabilities_less_current_portion, 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( w1.current_portion_of_operating_lease_liabilities, 0 )
		  + coalesce( w1.current_portion_of_long_term_debt, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.operating_lease_liabilities_less_current_portion, 0 ) ) / 1000
		  as "total_debt"
		, w1.total_liabilities / 1000 as "total_liabilities"
		, ( coalesce( w1.common_stock_0_0001_par_value_220000000_shares_authorized_38426669_shares_issued_and_outstanding, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000
		  as "total_paid_in_capital"
		, null / 1000 as "treasury_stock"
		, w1.accumulated_deficit / 1000 as "retained_earnings"
		, w1.total_stockholders_equity as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity as "total_liabilities_and_equity"
	from
		"10_K2021_03_16_CONSOLIDATED_AND_COMBINED_BALANCE_SHEETS_13888021745989" w1
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2021_03_16_CONSOLIDATED_AND_COMBINED_BALANCE_SHEETS_13888021745989" a1
		 	order by
		 		 a1.date
		 	limit 1
		 )
) e1
order by
	 e1.date
;


--TO VIEW COMPILED INCOME STATEMENT
drop table temp_income_statement;
create temporary table temp_income_statement as
select
	 ( select cik from temp_cik ) as "cik"
	, e1.*
from(
	--INSERT QUERY HERE
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.product_revenues_net as float ) / 1000 as "total_sales"
		, - cast( w1.cost_of_goods_sold as float ) / 1000 as "total_cost_of_sales"
		, - cast( w1.selling_general_and_administrative_expenses as float ) / 1000 as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.operating_income_loss as float ) / 1000 as "operating_income"
		, cast( w1.interest_expense_net as float ) / 1000 as "net_interest_expense"
		, null / 1000 as "interest_expense"
		, - cast( w1.benefit_for_income_taxes as float ) / 1000 as "income_tax"
		, cast( w1.net_income_loss as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_income_loss as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2022_03_14_CONSOLIDATED_AND_COMBINED_STATEMENTS_OF_OPERATIONS_13888022737458" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding_in_shares / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2022_03_14_CONSOLIDATED_AND_COMBINED_BALANCE_SHEETS_PARENTHETICAL_13888022737458" w1
		where
			 w1.common_stock_shares_outstanding_in_shares is not null
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in( '2021-12-31', '2019-12-31' )
		 	/*select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2022_03_14_CONSOLIDATED_AND_COMBINED_STATEMENTS_OF_OPERATIONS_13888022737458" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )*/
--		union all
) e1
order by
	 e1.date
;



--=======================================
--	CALL OUT FINANCIAL STATEMENTS
--=======================================

with
	temp_is as(
		select
			 w1.*
		from
			temp_income_statement w1
	)
	,
	temp_bs as(
		select
			 w1.*
		from
			temp_balance_sheet w1
	)
select
	 w1.cik
	, w1.date
	, w1.common_stock_outstanding
	, w1.total_sales
	, w1.total_sales + w1.total_cost_of_sales as "gp"
	, w1.selling_general_and_administrative_expense as "sga"
	, round( cast( ( w1.total_sales + w1.total_cost_of_sales ) as float ) / w1.total_sales * 100, 1 ) as "gpm_pct"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.total_sales * 100, 1 ) as "sga_pct"
	, round( cast( w1.operating_income as float ) / w1.total_sales * 100, 1 ) as "op_earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w1.common_stock_outstanding, 1 ) as "sales_per_share"
	, round( cast( w1.total_sales + w1.total_cost_of_sales as float ) / w1.common_stock_outstanding, 1 ) as "gp_per_share"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.common_stock_outstanding, 1 ) as "sga_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.common_stock_outstanding, 1 ) as "earnings_per_share"
from
	temp_is w1
order by
	 w1.date
;
select
	 w1.*
from
	temp_bs w1
;





--=========================
--	DROP TEMP TABLES
--=========================

drop table temp_cik;
drop table temp_sqlitemaster;
drop table temp_balance_sheet;
drop table temp_income_statement;
