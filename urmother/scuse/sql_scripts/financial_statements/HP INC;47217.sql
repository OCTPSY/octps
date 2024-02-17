--================================
--	COMPANY NAME : HP INC			--UPDATE HERE
--	CENTRAL INDEX KEY : 47217		--UPDATE HERE
--================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '47217' as "cik"		--UPDATE HERE
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
	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "bs_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
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
	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "is_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
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
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, null as "financing_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment_net as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.notes_payable_and_short_term_borrowings as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, coalesce( w1.notes_payable_and_short_term_borrowings, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  as "total_debt"
		, coalesce( w1.total_current_liabilities, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.other_non_current_liabilities, 0 )
		  as "total_liabilities"
		, coalesce( w1.common_stock_0_01_par_value_9600_shares_authorized_1304_and_1458_shares_issued_and_outstanding_at_october_31_2020_and_2019_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.accumulated_deficit as "retained_earnings"
		, w1.total_stockholders_deficit as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_deficit as "total_liabilities_and_equity"
	from
		"10_K2020_12_10_CONSOLIDATED_BALANCE_SHEETS_104423201380698" w1
		union all
	select
		 w1.date
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, null as "financing_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment_net as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.notes_payable_and_short_term_borrowings as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, coalesce( w1.notes_payable_and_short_term_borrowings, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  as "total_debt"
		, coalesce( w1.total_current_liabilities, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.other_non_current_liabilities, 0 )
		  as "total_liabilities"
		, coalesce( w1.common_stock_0_01_par_value_9600_shares_authorized_1560_and_1650_shares_issued_and_outstanding_at_october_31_2018_and_2017_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.accumulated_deficit as "retained_earnings"
		, w1.total_stockholders_deficit as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_deficit as "total_liabilities_and_equity"
	from
		"10_K2018_12_13_CONSOLIDATED_BALANCE_SHEETS_104423181233269" w1
		union all
	select
		 w1.date
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable as "accounts_receivable_net"
		, null as "financing_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, null as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.notes_payable_and_short_term_borrowings as "short_term_debt"
		, null as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, coalesce( w1.notes_payable_and_short_term_borrowings, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  as "total_debt"
		, coalesce( w1.total_current_liabilities, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.other_non_current_liabilities, 0 )
		  as "total_liabilities"
		, coalesce( w1.common_stock_0_01_par_value_9600_shares_authorized_1712_and_1804_shares_issued_and_outstanding_at_october_31_2016_and_2015_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.retained_deficit_earnings as "retained_earnings"
		, w1.total_hp_stockholders_deficit_equity as "total_shareholder_equity"
		, w1.non_controlling_interests_of_discontinued_operations as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_deficit_equity as "total_liabilities_and_equity"
	from
		"10_K2016_12_15_CONSOLIDATED_BALANCE_SHEETS_104423162054161" w1
		union all
	select
		 w1.date
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable as "accounts_receivable_net"
		, coalesce( w1.financing_receivables, 0 )
		  + coalesce( w1.long_term_financing_receivables_and_other_assets, 0 )
		  as "financing_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, w1.intangible_assets as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.notes_payable_and_short_term_borrowings as "short_term_debt"
		, w1.deferred_revenue as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, coalesce( w1.notes_payable_and_short_term_borrowings, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  as "total_debt"
		, coalesce( w1.total_current_liabilities, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.other_liabilities, 0 )
		  as "total_liabilities"
		, coalesce( w1.common_stock_0_01_par_value_9600_shares_authorized_1839_and_1908_shares_issued_and_outstanding_at_october_31_2014_and_october_31_2013_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.retained_earnings as "retained_earnings"
		, w1.total_hp_stockholders_equity as "total_shareholder_equity"
		, w1.non_controlling_interests as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity as "total_liabilities_and_equity"
	from
		"10_K2014_12_18_CONSOLIDATED_BALANCE_SHEETS_104423141293844" w1
		union all
	select
		 w1.date
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable as "accounts_receivable_net"
		, coalesce( w1.financing_receivables, 0 )
		  + coalesce( w1.long_term_financing_receivables_and_other_assets, 0 )
		  as "financing_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, w1.property_plant_and_equipment as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, w1.purchased_intangible_assets as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.notes_payable_and_short_term_borrowings as "short_term_debt"
		, w1.deferred_revenue as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, w1.total_current_liabilities as "total_current_liabilities"
		, null as "long_term_unearned_revenue"
		, w1.long_term_debt as "long_term_debt"
		, coalesce( w1.notes_payable_and_short_term_borrowings, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  as "total_debt"
		, coalesce( w1.total_current_liabilities, 0 )
		  + coalesce( w1.long_term_debt, 0 )
		  + coalesce( w1.other_liabilities, 0 )
		  as "total_liabilities"
		, coalesce( w1.common_stock_0_01_par_value_9600_shares_authorized_1963_and_1991_shares_issued_and_outstanding_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.retained_earnings as "retained_earnings"
		, w1.total_hp_stockholders_equity as "total_shareholder_equity"
		, w1.non_controlling_interests as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity as "total_liabilities_and_equity"
	from
		"10_K2012_12_27_CONSOLIDATED_BALANCE_SHEETS_104423121287831" w1
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
		, w1.net_revenue as "total_sales"
		, - w1.cost_of_revenue as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, - null as "depreciation_amortisation"
		, - null as "capex"
		, w1.earnings_from_operations as "operating_income"
		, w1.interest_and_other_net as "net_interest_expense"
		, w1.provision_for_benefit_from_taxes as "income_tax"
		, w1.net_earnings as "continuing_net_earnings_total"
		, null as "discontinued_net_earnings_total"
		, w1.net_earnings as "net_earnings_to_shareholder"
	from
		"10_K2020_12_10_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423201380698" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding_in_shares / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2020_12_10_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_104423201380698" w1
		where
			 w1.common_stock_shares_outstanding_in_shares is not null
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
		 		"10_K2020_12_10_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423201380698" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1."net_revenue.1" as "total_sales"
		, - w1.cost_of_revenue as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, - null as "depreciation_amortisation"
		, - null as "capex"
		, w1.earnings_from_continuing_operations as "operating_income"
		, w1.interest_and_other_net as "net_interest_expense"
		, w1.benefit_from_provision_for_taxes as "income_tax"
		, w1.net_earnings_from_continuing_operations as "continuing_net_earnings_total"
		, w1.net_loss_from_discontinued_operations as "discontinued_net_earnings_total"
		, w1.net_earnings as "net_earnings_to_shareholder"
	from
		"10_K2018_12_13_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423181233269" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2018_12_13_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_104423181233269" w1
		where
			 w1.common_stock_shares_outstanding is not null
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
		 		"10_K2018_12_13_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423181233269" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1."net_revenue.1" as "total_sales"
		, - w1.cost_of_revenue as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, - null as "depreciation_amortisation"
		, - null as "capex"
		, w1.earnings_from_continuing_operations as "operating_income"
		, w1.interest_and_other_net as "net_interest_expense"
		, w1.provision_for_benefit_from_taxes as "income_tax"
		, w1.net_earnings_from_continuing_operations as "continuing_net_earnings_total"
		, w1.net_loss_earnings_from_discontinued_operations as "discontinued_net_earnings_total"
		, w1.net_earnings as "net_earnings_to_shareholder"
	from
		"10_K2016_12_15_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423162054161" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding, 1 ) as "common_stock_outstanding"
		from
			"10_K2016_12_15_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_104423162054161" w1
		where
			 w1.common_stock_shares_outstanding is not null
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
		 		"10_K2016_12_15_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423162054161" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1.total_net_revenue as "total_sales"
		, - coalesce( w1.cost_of_products, 0 )
		  - coalesce( w1.cost_of_services, 0 )
		  as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, - null as "depreciation_amortisation"
		, - null as "capex"
		, w1.earnings_loss_from_operations as "operating_income"
		, w1.interest_and_other_net as "net_interest_expense"
		, w1.provision_for_taxes as "income_tax"
		, w1.net_earnings_loss as "continuing_net_earnings_total"
		, null as "discontinued_net_earnings_total"
		, w1.net_earnings_loss as "net_earnings_to_shareholder"
	from
		"10_K2014_12_18_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423141293844" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( w1.common_stock_shares_outstanding, 1 ) as "common_stock_outstanding"
		from
			"10_K2014_12_18_CONSOLIDATED_CONDENSED_BALANCE_SHEETS_PARENTHETICAL_104423141293844" w1
		where
			 w1.common_stock_shares_outstanding is not null
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
		 		"10_K2014_12_18_CONSOLIDATED_STATEMENTS_OF_EARNINGS_104423141293844" a1
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
