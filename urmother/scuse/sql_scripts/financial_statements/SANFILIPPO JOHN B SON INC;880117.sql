--=================================================
--	COMPANY NAME : SANFILIPPO JOHN B & SON INC		--UPDATE HERE
--	CENTRAL INDEX KEY : 880117						--UPDATE HERE
--=================================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '880117' as "cik"		--UPDATE HERE
	, '%balance_sheets_%' as "bs_include"
	, '%balance_sheets_parenthetical%' as "bs_exclude"
	, '%statements_of_comprehensive_income_%' as "is_include"
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
		, cast( w1.cash as float ) / 1000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_291_and_391_respectively as float ) / 1000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, ( coalesce( cast( w1.total_property_plant_and_equipment as float ), 0 )
		  + coalesce( cast( w1.operating_lease_right_of_use_assets as float ), 0 ) ) / 1000
		  as "property_plant_and_equipment_net"
		, cast( w1.goodwill as float ) / 1000 as "goodwill"
		, cast( w1.intangible_assets_net as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_627_and_585_respectively_and_net_of_unamortized_debt_issuance_costs_of_15_and_25_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 ) ) / 1000
		  as "short_term_debt"
		, null / 1000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, null / 1000 as "long_term_unearned_revenue"
		, ( coalesce( cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_8320_and_8947_respectively_and_net_of_unamortized_debt_issuance_costs_of_4_and_19_respectively as float ), 0 )
		  + coalesce( cast( w1.long_term_operating_lease_liabilities_net_of_current_portion as float ), 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_627_and_585_respectively_and_net_of_unamortized_debt_issuance_costs_of_15_and_25_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 )
		  + coalesce( cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_8320_and_8947_respectively_and_net_of_unamortized_debt_issuance_costs_of_4_and_19_respectively as float ), 0 )
		  + coalesce( cast( w1.long_term_operating_lease_liabilities_net_of_current_portion as float ), 0 ) ) / 1000
		  as "total_debt"
		, cast( w1.total_liabilities as float ) / 1000 as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_117900_shares_of_common_stock as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000 as "total_shareholder_equity"
		, null / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_stockholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2021_08_18_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_UNAUDITED_19681211186957" w1
		union all
	select
		 w1.date
		, cast( w1.cash as float ) / 1000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_350_and_270_respectively as float ) / 1000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, cast( w1.total_property_plant_and_equipment as float ) / 1000 as "property_plant_and_equipment_net"
		, cast( w1.goodwill as float ) / 1000 as "goodwill"
		, cast( w1.intangible_assets_net as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_4375_and_4341_respectively_and_net_of_unamortized_debt_issuance_costs_of_35_and_45_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 ) ) / 1000
		  as "short_term_debt"
		, null / 1000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, null / 1000 as "long_term_unearned_revenue"
		, cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_11495_and_15507_respectively_and_net_of_unamortized_debt_issuance_costs_of_44_and_79_respectively as float ) / 1000 as "long_term_debt"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_4375_and_4341_respectively_and_net_of_unamortized_debt_issuance_costs_of_35_and_45_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 )
		  + coalesce( cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_11495_and_15507_respectively_and_net_of_unamortized_debt_issuance_costs_of_44_and_79_respectively as float ), 0 ) ) / 1000
		  as "total_debt"
		, cast( w1.total_liabilities as float ) / 1000 as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_117900_shares_of_common_stock as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000 as "total_shareholder_equity"
		, null / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_stockholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2019_08_21_CONSOLIDATED_BALANCE_SHEETS_19681191043222" w1
		union all
	select
		 w1.date
		, cast( w1.cash as float ) / 1000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_263_and_397_respectively as float ) / 1000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000 as "total_current_assets"
		, cast( w1.total_property_plant_and_equipment as float ) / 1000 as "property_plant_and_equipment_net"
		, null / 1000 as "goodwill"
		, cast( w1.intangible_assets_net as float ) / 1000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000 as "total_assets"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_474_and_407_respectively_and_net_of_unamortized_debt_issuance_costs_of_55_and_65_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 ) ) / 1000
		  as "short_term_debt"
		, null / 1000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable_including_related_party_payables_of_178_and_113_respectively as float ) / 1000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000 as "total_current_liabilities"
		, null / 1000 as "long_term_unearned_revenue"
		, cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_10584_and_11133_respectively_and_net_of_unamortized_debt_issuance_costs_of_124_and_179_respectively as float ) / 1000 as "long_term_debt"
		, ( coalesce( cast( w1.revolving_credit_facility_borrowings as float ), 0 )
		  + coalesce( cast( w1.current_maturities_of_long_term_debt_including_related_party_debt_of_474_and_407_respectively_and_net_of_unamortized_debt_issuance_costs_of_55_and_65_respectively as float ), 0 )
		  + coalesce( cast( w1.bank_overdraft as float ), 0 )
		  + coalesce( cast( w1.long_term_debt_less_current_maturities_including_related_party_debt_of_10584_and_11133_respectively_and_net_of_unamortized_debt_issuance_costs_of_124_and_179_respectively as float ), 0 ) ) / 1000
		  as "total_debt"
		, cast( w1.total_liabilities as float ) / 1000 as "total_liabilities"
		, cast( w1.capital_in_excess_of_par_value as float ) / 1000 as "total_paid_in_capital"
		, cast( w1.treasury_stock_at_cost_117900_shares_of_common_stock as float ) / 1000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000 as "total_shareholder_equity"
		, null / 1000 as "non_controlling_interest"
		, cast( w1.total_liabilities_stockholders_equity as float ) / 1000 as "total_liabilities_and_equity"
	from
		"10_K2017_08_23_CONSOLIDATED_BALANCE_SHEETS_19681171047284" w1
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
		, cast( w1.net_sales as float ) / 1000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.selling_expenses as float ), 0 )
		  - coalesce( cast( w1.administrative_expenses as float ), 0 ) ) / 1000
		  as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.income_from_operations as float ) / 1000 as "operating_income"
		, - cast( w1.interest_expense_including_653_821_and_1143_to_related_parties_respectively as float ) / 1000 as "interest_expense"
		, - cast( w1.income_tax_expense as float ) / 1000 as "income_tax"
		, cast( w1.net_income as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_income as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2021_08_18_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_UNAUDITED_19681211186957" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( coalesce( cast( w1.common_stock_shares_outstanding as float ) + cast( w1."common_stock_shares_issued.1" as float ), 0 ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2021_08_18_CONSOLIDATED_BALANCE_SHEETS_UNAUDITED_PARENTHETICAL_19681211186957" w1
		where
			 ( coalesce( w1.common_stock_shares_outstanding, 0 ) + coalesce( w1."common_stock_shares_issued.1", 0 ) ) != 0
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
		 		"10_K2021_08_18_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_UNAUDITED_19681211186957" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.net_sales as float ) / 1000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.selling_expenses as float ), 0 )
		  - coalesce( cast( w1.administrative_expenses as float ), 0 ) ) / 1000
		  as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.income_from_operations as float ) / 1000 as "operating_income"
		, - cast( w1.interest_expense_including_1143_1103_and_785_to_related_parties_respectively as float ) / 1000 as "interest_expense"
		, - cast( w1.income_tax_expense as float ) / 1000 as "income_tax"
		, cast( w1.net_income as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_income as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2019_08_21_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_19681191043222" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( coalesce( cast( w1.common_stock_shares_outstanding as float ) + cast( w1."common_stock_shares_issued.1" as float ), 0 ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2019_08_21_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_19681191043222" w1
		where
			 ( coalesce( w1.common_stock_shares_outstanding, 0 ) + coalesce( w1."common_stock_shares_issued.1", 0 ) ) != 0
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
		 		"10_K2019_08_21_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_19681191043222" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.net_sales as float ) / 1000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.selling_expenses as float ), 0 )
		  - coalesce( cast( w1.administrative_expenses as float ), 0 ) ) / 1000
		  as "selling_general_and_administrative_expense"
		, null / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, cast( w1.income_from_operations as float ) / 1000 as "operating_income"
		, - cast( w1.interest_expense_including_785_1081_and_1110_to_related_parties_respectively as float ) / 1000 as "interest_expense"
		, - cast( w1.income_tax_expense as float ) / 1000 as "income_tax"
		, cast( w1.net_income as float ) / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, cast( w1.net_income as float ) / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2017_08_23_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_19681171047284" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( coalesce( cast( w1.common_stock_shares_outstanding as float ) + cast( w1."common_stock_shares_issued.1" as float ), 0 ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2017_08_23_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_19681171047284" w1
		where
			 ( coalesce( w1.common_stock_shares_outstanding, 0 ) + coalesce( w1."common_stock_shares_issued.1", 0 ) ) != 0
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
		 		"10_K2017_08_23_CONSOLIDATED_STATEMENTS_OF_COMPREHENSIVE_INCOME_19681171047284" a1
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
