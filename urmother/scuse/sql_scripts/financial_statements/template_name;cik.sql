--================================
--	COMPANY NAME : TEXTRON INC		--UPDATE HERE
--	CENTRAL INDEX KEY : 217346		--UPDATE HERE
--================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '217346' as "cik"		--UPDATE HERE
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
		, w1.cash_and_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, w1."inventories.1" as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment_net as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1."assets.2" as "total_assets"
		, w1.current_portion_of_long_term_debt as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, w1.debt as "total_debt"
		, w1."total_liabilities.1" as "total_liabilities"
		, coalesce( w1.common_stock_219_2_million_and_231_0_million_shares_issued_respectively_and_216_9_million_and_226_4_million_shares_outstanding_respectively, 0 )
		  + coalesce( w1.capital_surplus, 0 )
		  as "total_paid_in_capital"
		, w1.treasury_stock as "treasury_stock"
		, w1.retained_earnings as "retained_earnings"
		, w1.total_shareholders_equity as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_shareholders_equity as "total_liabilities_and_equity"
	from
		"10_K2022_02_17_CONSOLIDATED_BALANCE_SHEETS_10548022646246" w1
		union all
	select
		 w1.date
		, w1.cash_and_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, w1."inventories.1" as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment_net as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1."total_assets.1" as "total_assets"
		, w1.current_portion_of_long_term_debt as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, w1.debt as "total_debt"
		, w1."total_liabilities.1" as "total_liabilities"
		, coalesce( w1.common_stock_228_4_million_and_238_2_million_shares_issued_respectively_and_228_0_million_and_235_6_million_shares_outstanding_respectively, 0 )
		  + coalesce( w1.capital_surplus, 0 )
		  as "total_paid_in_capital"
		, w1.treasury_stock as "treasury_stock"
		, w1.retained_earnings as "retained_earnings"
		, w1.total_shareholders_equity as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_shareholders_equity as "total_liabilities_and_equity"
	from
		"10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_10548020650740" w1
		union all
	select
		 w1.date
		, w1."cash_and_equivalents.1" as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, w1."inventories" as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment_net as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1."total_assets.1" as "total_assets"
		, w1.short_term_debt_and_current_portion_of_long_term_debt as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, w1.debt as "total_debt"
		, w1."total_liabilities.1" as "total_liabilities"
		, coalesce( w1.common_stock_270_3_million_and_288_3_million_shares_issued_respectively_and_270_3_million_and_274_2_million_shares_outstanding_respectively, 0 )
		  + coalesce( w1.capital_surplus, 0 )
		  as "total_paid_in_capital"
		, w1.treasury_stock as "treasury_stock"
		, w1.retained_earnings as "retained_earnings"
		, w1.total_shareholders_equity as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_shareholders_equity as "total_liabilities_and_equity"
	from
		"10_K2017_02_22_CONSOLIDATED_BALANCE_SHEETS_10548017627969" w1
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
		, w3.common_stock_outstanding
		, w1.total_revenues as "total_sales"
		, - coalesce( w1.total_costs_expenses_and_other, 0 )
		  + coalesce( w1.selling_and_administrative_expense, 0 )
		  + coalesce( w1.interest_expense, 0 )
		  + coalesce( w1.special_charges, 0 )
		  + coalesce( non_service_components_of_pension_and_postretirement_income_net, 0 )
		  + coalesce( gain_on_business_disposition, 0 )
		  as "total_cost_of_sales"
		, - w1.selling_and_administrative_expense as "selling_general_and_administrative_expense"
		, null as "research_and_development"
		, - w2.depreciation_and_amortization as "depreciation_amortisation"
		, - w2.capital_expenditures as "capex"
		, coalesce( w1.income_from_continuing_operations_before_income_taxes, 0 )
		  + coalesce( w1.interest_expense, 0 )
		  as "operating_income"
		, - w1.interest_expense as "interest_expense"
		, - w1.income_tax_expense_benefit as "income_tax"
		, w1.income_from_continuing_operations as "continuing_net_earnings_total"
		, w1.loss_from_discontinued_operations as "discontinued_net_earnings_total"
		, w1.net_income as "net_earnings_to_shareholder"
	from
		"10_K2022_02_17_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10548022646246" 	w1
	left join
		"10_K2022_02_17_SEGMENT_AND_GEOGRAPHIC_DATA_ASSETS_CAPITAL_EXPENDITURES_AND_DEPRECIATION_AND_AMORTIZATION_BY_SEGMENT_DETAILS_10548022646246" w2 on
			 w1.date = w2.date
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_outstanding_in_shares / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2022_02_17_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10548022646246" w1
		where
			 w1.common_stock_outstanding_in_shares is not null
		order by
			 w1.date desc
		limit 3
	)																			w3 on
			 w1.date = w3.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2022_02_17_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10548022646246" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w3.common_stock_outstanding
		, w1.total_revenues as "total_sales"
		, - w1.cost_of_sales as "total_cost_of_sales"
		, - w1.selling_and_administrative_expense as "selling_general_and_administrative_expense"
		, null as "research_and_development"
		, - w2.depreciation_and_amortization as "depreciation_amortisation"
		, - w2.capital_expenditures as "capex"
		, coalesce( w1.income_from_continuing_operations_before_income_taxes, 0 )
		  + coalesce( w1.interest_expense, 0 )
		  as "operating_income"
		, - w1.interest_expense as "interest_expense"
		, - w1.income_tax_expense as "income_tax"
		, w1.income_from_continuing_operations as "continuing_net_earnings_total"
		, w1.income_from_discontinued_operations_net_of_income_taxes as "discontinued_net_earnings_total"
		, w1.net_income as "net_earnings_to_shareholder"
	from
		"10_K2020_02_25_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10548020650740" w1
	left join
		"10_K2020_02_25_SEGMENT_AND_GEOGRAPHIC_DATA_ASSETS_CAPITAL_EXPENDITURES_AND_DEPRECIATION_AND_AMORTIZATION_BY_SEGMENT_DETAILS_10548020650740" w2 on
			 w1.date = w2.date
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10548020650740" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 w1.date desc
		limit 3
	)																			w3 on
			 w1.date = w3.end_of_month
		union all
	select
		 w1.date
		, w3.common_stock_outstanding
		, w1.total_revenues as "total_sales"
		, - w1.cost_of_sales as "total_cost_of_sales"
		, - w1.selling_and_administrative_expense as "selling_general_and_administrative_expense"
		, null as "research_and_development"
		, - w2.depreciation_and_amortization as "depreciation_amortisation"
		, - w2.capital_expenditures as "capex"
		, coalesce( w1.income_from_continuing_operations_before_income_taxes, 0 )
		  + coalesce( w1.interest_expense, 0 )
		  as "operating_income"
		, - w1.interest_expense as "interest_expense"
		, - w1.income_tax_expense as "income_tax"
		, w1.income_from_continuing_operations as "continuing_net_earnings_total"
		, w1.income_loss_from_discontinued_operations_net_of_income_taxes as "discontinued_net_earnings_total"
		, w1.net_income as "net_earnings_to_shareholder"
	from
		"10_K2017_02_22_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10548017627969" w1
	left join
		"10_K2017_02_22_SEGMENT_AND_GEOGRAPHIC_DATA_ASSETS_CAPITAL_EXPENDITURES_AND_DEPRECIATION_AND_AMORTIZATION_BY_SEGMENT_DETAILS_10548017627969" w2 on
			 w1.date = w2.date
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2017_02_22_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10548017627969" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 w1.date desc
		limit 3
	)																			w3 on
			 w1.date = w3.end_of_month
	where
		 lower( w1.date ) not like '%consolidated%'
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
	, round( cast( ( w1.total_sales + w1.total_cost_of_sales ) as float ) / w1.total_sales * 100, 2 ) as "gpm_pct"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.total_sales * 100, 2 ) as "sga_pct"
	, round( cast( w1.operating_income as float ) / w1.total_sales * 100, 2 ) as "op_earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w1.common_stock_outstanding, 2 ) as "sales_per_share"
	, round( cast( w1.total_sales + w1.total_cost_of_sales as float ) / w1.common_stock_outstanding, 2 ) as "gp_per_share"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.common_stock_outstanding, 2 ) as "sga_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.common_stock_outstanding, 2 ) as "earnings_per_share"
	, round( cast( w2.total_shareholder_equity as float ) / w1.common_stock_outstanding, 2 ) as "equity_per_share"
	, round( cast( w2.total_debt as float ) / w1.common_stock_outstanding, 2 ) as "debt_per_share"
	, round( cast( w2.accounts_receivable_net as float ) / w1.common_stock_outstanding, 2 ) as "ar_per_share"
	, round( cast( w2.inventories as float ) / w1.common_stock_outstanding, 2 ) as "invntry_per_share"
	, round( cast( w2.accounts_payable as float ) / w1.common_stock_outstanding, 2 ) as "ap_per_share"
	, round( cast( w2.accounts_receivable_net as float ) / w1.total_sales * 365, 2 ) as "dso"
	, round( cast( w2.inventories as float ) / w1.total_sales * 365, 2 ) as "dsi"
	, round( cast( w2.accounts_payable as float ) / w1.total_sales * 365, 2 ) as "dsp"
	, w2.total_current_assets
	, w2.total_current_liabilities
	, w2.total_current_assets - w2.total_current_liabilities as "working_capital"
	, round(
	  	 cast( w2.total_current_assets - w2.total_current_liabilities as float )
	  	 / w1.common_stock_outstanding
	  	, 2
	  ) as "wc_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w2.total_shareholder_equity * 100, 2 ) as "roe"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.total_sales * 100, 2 ) as "earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w2.total_assets, 2 ) as "asset_turnover"
	, round( cast( w2.total_assets as float ) / w2.total_shareholder_equity, 2 ) as "equity_multiplier"
from
	temp_is w1
left join
	temp_bs w2 on
		 w1.date = w2.date
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
