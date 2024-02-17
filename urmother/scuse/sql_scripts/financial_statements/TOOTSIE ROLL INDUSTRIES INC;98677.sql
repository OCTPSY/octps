--=================================================
--	COMPANY NAME : TOOTSIE ROLL INDUSTRIES INC		--UPDATE HERE
--	CENTRAL INDEX KEY : 98677						--UPDATE HERE
--=================================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '98677' as "cik"		--UPDATE HERE
	, '%financial_position_%' as "bs_include"
	, '%financial_position_parenthetical%' as "bs_exclude"
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
		, ( coalesce( cast( w1.cash_and_cash_equivalents as float ), 0 )
		  + coalesce( cast( w1.restricted_cash as float ), 0 ) ) / 1000
		  as "cash_and_equivalents"
		, ( coalesce( cast( w1.investments as float ), 0 )
		  + coalesce( cast( w1."investments.1" as float ), 0 ) ) / 1000
		  as "investments"
		, cast( w1.accounts_receivable_trade_less_allowances_of_2281_and_1694 as float ) / 1000 as "accounts_receivable_net"
		, ( coalesce( cast( w1.finished_goods_and_work_in_process as float ), 0 )
		  + coalesce( cast( w1.raw_materials_and_supplies as float ), 0 ) ) / 1000
		  as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, cast( w1.net_property_plant_and_equipment as float ) / 1000 as "property_plant_and_equipment_net"
		, cast( w1.goodwill as float ) / 1000 as "goodwill"
		, cast( w1.trademarks as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, ( coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.operating_lease_liabilities as float ), 0 ) ) / 1000
		  as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, ( coalesce( cast( w1.industrial_development_bonds as float ), 0 )
		  + coalesce( cast( w1."operating_lease_liabilities.1" as float ), 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.operating_lease_liabilities as float ), 0 )
		  + coalesce( cast( w1.industrial_development_bonds as float ), 0 )
		  + coalesce( cast( w1."operating_lease_liabilities.1" as float ), 0 ) ) / 1000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.total_noncurrent_liabilities as float ), 0 ) ) / 1000
		  as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_96_shares_and_93_shares_respectively as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_tootsie_roll_industries_inc_shareholders_equity as float ) / 1000 as "total_shareholder_equity"
		, cast( w1.noncontrolling_interests as float ) / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_shareholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2022_03_01_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_10136122694653" w1
		union all
	select
		 w1.date
		, ( coalesce( cast( w1.cash_and_cash_equivalents as float ), 0 )
		  + coalesce( cast( w1.restricted_cash as float ), 0 ) ) / 1000
		  as "cash_and_equivalents"
		, ( coalesce( cast( w1.investments as float ), 0 )
		  + coalesce( cast( w1."investments.1" as float ), 0 ) ) / 1000
		  as "investments"
		, cast( w1.accounts_receivable_trade_less_allowances_of_1949_and_1820 as float ) / 1000 as "accounts_receivable_net"
		, ( coalesce( cast( w1.finished_goods_and_work_in_process as float ), 0 )
		  + coalesce( cast( w1.raw_materials_and_supplies as float ), 0 ) ) / 1000
		  as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, cast( w1.net_property_plant_and_equipment as float ) / 1000 as "property_plant_and_equipment_net"
		, cast( w1.goodwill as float ) / 1000 as "goodwill"
		, cast( w1.trademarks as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, ( coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.operating_lease_liabilities as float ), 0 ) ) / 1000
		  as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, ( coalesce( cast( w1.industrial_development_bonds as float ), 0 )
		  + coalesce( cast( w1."operating_lease_liabilities.1" as float ), 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.operating_lease_liabilities as float ), 0 )
		  + coalesce( cast( w1.industrial_development_bonds as float ), 0 )
		  + coalesce( cast( w1."operating_lease_liabilities.1" as float ), 0 ) ) / 1000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.total_noncurrent_liabilities as float ), 0 ) ) / 1000
		  as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_90_shares_and_88_shares_respectively as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_tootsie_roll_industries_inc_shareholders_equity as float ) / 1000 as "total_shareholder_equity"
		, cast( w1.noncontrolling_interests as float ) / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_shareholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_10136120669964" w1
		union all
	select
		 w1.date
		, ( coalesce( cast( w1.cash_cash_equivalents as float ), 0 )
		  + coalesce( cast( w1.restricted_cash as float ), 0 ) ) / 1000
		  as "cash_and_equivalents"
		, ( coalesce( cast( w1.investments as float ), 0 )
		  + coalesce( cast( w1."investments.1" as float ), 0 ) ) / 1000
		  as "investments"
		, cast( w1.accounts_receivable_trade_less_allowances_of_1921_and_1884 as float ) / 1000 as "accounts_receivable_net"
		, ( coalesce( cast( w1.finished_goods_work_in_process as float ), 0 )
		  + coalesce( cast( w1.raw_material_supplies as float ), 0 ) ) / 1000
		  as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, cast( w1.net_property_plant_and_equipment as float ) / 1000 as "property_plant_and_equipment_net"
		, cast( w1.goodwill as float ) / 1000 as "goodwill"
		, cast( w1.trademarks as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, cast( w1.bank_loan as float ) / 1000 as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, ( coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.industrial_development_bonds as float ), 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( cast( w1.bank_loan as float ), 0 )
		  + coalesce( cast( w1.bank_loans as float ), 0 )
		  + coalesce( cast( w1.industrial_development_bonds as float ), 0 ) ) / 1000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.total_noncurrent_liabilities as float ), 0 ) ) / 1000
		  as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_85_83_shares_respectively as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_tootsie_roll_industries_inc_shareholders_equity as float ) / 1000 as "total_shareholder_equity"
		, cast( w1.noncontrolling_interests as float ) / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_shareholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2018_03_01_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_10136118655238" w1
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
		, cast( w1.revenue as float ) / 1000 as "total_sales"
		, - cast( w1.costs as float ) / 1000 as "total_cost_of_sales"
		, - cast( w1.selling_marketing_and_administrative_expenses as float ) / 1000 as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.earnings_from_operations as float ) / 1000 as "operating_income"
		, null / 1000 as "interest_expense"
		, - cast( w1.provision_for_income_taxes as float ) / 1000 as "income_tax"
		, cast( w1.net_earnings as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_earnings_attributable_to_tootsie_roll_industries_inc as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2022_03_01_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136122694653" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2022_03_01_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_PARENTHETICAL_10136122694653" w1
		where
			 cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) != 0
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2022_03_01_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136122694653" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.revenue as float ) / 1000 as "total_sales"
		, - cast( w1.costs as float ) / 1000 as "total_cost_of_sales"
		, - cast( w1.selling_marketing_and_administrative_expenses as float ) / 1000 as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.earnings_from_operations as float ) / 1000 as "operating_income"
		, null / 1000 as "interest_expense"
		, - cast( w1.provision_for_income_taxes as float ) / 1000 as "income_tax"
		, cast( w1.net_earnings as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_earnings_attributable_to_tootsie_roll_industries_inc as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136120669964" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_PARENTHETICAL_10136120669964" w1
		where
			 cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) != 0
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136120669964" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.total_revenue as float ) / 1000 as "total_sales"
		, - cast( w1.total_costs as float ) / 1000 as "total_cost_of_sales"
		, - cast( w1.selling_marketing_and_administrative_expenses as float ) / 1000 as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.earnings_from_operations as float ) / 1000 as "operating_income"
		, null / 1000 as "interest_expense"
		, - cast( w1.provision_for_income_taxes as float ) / 1000 as "income_tax"
		, cast( w1.net_earnings as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_earnings_attributable_to_tootsie_roll_industries_inc as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2018_03_01_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136118655238" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) / 1000, 1 ) as "common_stock_outstanding"
		from
			"10_K2018_03_01_CONSOLIDATED_STATEMENTS_OF_FINANCIAL_POSITION_PARENTHETICAL_10136118655238" w1
		where
			 cast( w1.common_stock_shares_issued + w1."common_stock_shares_issued.1" as float ) != 0
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2018_03_01_CONSOLIDATED_STATEMENTS_OF_EARNINGS_AND_RETAINED_EARNINGS_10136118655238" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
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
