
--=====================================================
--	COMPANY NAME : SOUTHWEST AIRLINES CO
--	CENTRAL INDEX KEY : 92380
--=====================================================

select * from "10_K2022_02_07_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725922594925"
set common_stock_shares_issued_in_shares = 888111634
where date = '2020-12-31'
;


with
	temp_balance_sheet as(
		--UPDATE HERE
select 'LUV;92380' as "ticker_cik", '10_K2022_02_07_CONSOLIDATED_BALANCE_SHEET_10725922594925' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "accounts_and_other_receivables", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "inventories_of_parts_and_supplies_at_cost", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "property_and_equipment_net", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) ) / 1 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_1_00_par_value_2000000000_shares_authorized_888111634_shares_issued_in_2021_and_2020", 0 ) as float ) + cast( coalesce( "capital_in_excess_of_par_value", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_at_cost_295991525_and_297637297_shares_in_2021_and_2020_respectively", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) 
/ 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2022_02_07_CONSOLIDATED_BALANCE_SHEET_10725922594925" union all select 'LUV;92380' as "ticker_cik", '10_K2020_02_04_CONSOLIDATED_BALANCE_SHEET_10725920570045' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "accounts_and_other_receivables", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "inventories_of_parts_and_supplies_at_cost", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "property_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease_right_of_use_assets", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float 
) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) ) / 1 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) + cast( coalesce( "noncurrent_operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) + cast( coalesce( "noncurrent_operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_1_00_par_value_2000000000_shares_authorized_807611634_shares_issued_in_2019_and_2018", 0 ) as float ) + cast( coalesce( "capital_in_excess_of_par_value", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_at_cost_288547318_and_255008275_shares_in_2019_and_2018_respectively", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2020_02_04_CONSOLIDATED_BALANCE_SHEET_10725920570045" union all select 'LUV;92380' as "ticker_cik", '10_K2018_02_07_CONSOLIDATED_BALANCE_SHEET_10725918580264' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "accounts_and_other_receivables", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "inventories_of_parts_and_supplies_at_cost", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "property_and_equipment_net", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) ) / 1 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_debt_less_current_maturities", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_1_00_par_value_2000000000_shares_authorized_807611634_shares_issued_in_2017_and_2016", 0 ) as float ) + cast( coalesce( "capital_in_excess_of_par_value", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_at_cost_219060856_and_192450855_shares_in_2017_and_2016_respectively", 0 ) 
as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2018_02_07_CONSOLIDATED_BALANCE_SHEET_10725918580264"
	)
	,
	temp_balance_sheet_parenthetical as(
		--UPDATE HERE
select 'LUV;92380' as "ticker_cik", '10_K2022_02_07_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725922594925' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_shares_issued_in_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_at_cost_shares_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2022_02_07_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725922594925" union all select 'LUV;92380' as "ticker_cik", '10_K2020_02_04_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725920570045' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( 
null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_shares_issued_in_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_at_cost_shares_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2020_02_04_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725920570045" union all select 'LUV;92380' as "ticker_cik", '10_K2018_02_07_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725918580264' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_shares_issued_in_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_at_cost_shares_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2018_02_07_CONSOLIDATED_BALANCE_SHEET_PARENTHETICAL_10725918580264"
	)
	,
	temp_income_statement as(
		--UPDATE HERE
select 'LUV;92380' as "ticker_cik", '10_K2022_02_07_CONSOLIDATED_STATEMENT_OF_INCOME_LOSS_10725922594925' as "table_name", date as "date",  ( (  + cast( coalesce( "total_operating_revenues", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "fuel_and_oil", 0 ) as float ) - cast( coalesce( "maintenance_materials_and_repairs", 0 ) as float ) - cast( coalesce( "landing_fees_and_airport_rentals", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_wages_and_benefits", 0 ) as float ) - cast( coalesce( "payroll_support_and_voluntary_employee_programs_net", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income_loss", 0 ) as float ) ) / 1 ) as "operating_income", ( (  - cast( coalesce( "interest_expense", 
0 ) as float ) - cast( coalesce( "capitalized_interest", 0 ) as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "provision_benefit_for_income_taxes", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income_loss", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income_loss", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2022_02_07_CONSOLIDATED_STATEMENT_OF_INCOME_LOSS_10725922594925" union all select 'LUV;92380' as "ticker_cik", '10_K2020_02_04_CONSOLIDATED_STATEMENT_OF_INCOME_10725920570045' as "table_name", date as "date",  ( (  + cast( coalesce( "operating_revenue", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "fuel_and_oil", 0 ) as float ) - cast( coalesce( "maintenance_materials_and_repairs", 0 ) as float ) - cast( coalesce( "landing_fees_and_airport_rentals", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_wages_and_benefits", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income", 0 ) as float ) ) / 1 ) as "operating_income", ( (  - cast( coalesce( "interest_expense", 0 ) as float ) - cast( coalesce( "capitalized_interest", 0 ) as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "provision_benefit_for_income_taxes", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2020_02_04_CONSOLIDATED_STATEMENT_OF_INCOME_10725920570045" union all select 'LUV;92380' as "ticker_cik", '10_K2018_02_07_CONSOLIDATED_STATEMENT_OF_INCOME_10725918580264' as "table_name", date as "date",  ( (  + cast( coalesce( "total_operating_revenues", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "fuel_and_oil", 0 ) as float ) - cast( coalesce( "maintenance_materials_and_repairs", 0 ) as float ) - cast( coalesce( "aircraft_rentals", 0 ) as float ) - cast( coalesce( "landing_fees_and_other_rentals", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_wages_and_benefits", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income", 0 ) as float ) ) / 1 ) as "operating_income", ( (  - cast( coalesce( "interest_expense", 0 ) as float ) - cast( coalesce( "interest_costs_capitalized_adjustment", 0 ) as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "provision_for_income_taxes", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2018_02_07_CONSOLIDATED_STATEMENT_OF_INCOME_10725918580264"
	)
	,
	--TRANSFORMATION DONE FROM THIS POINT ONWARDS
	temp_bs_parent as(
		select
			 e1.*
		from(
				select
					 w1.ticker_cik
					, w1.date
					, w1.common_stock_shares_outstanding as "common_stock_outstanding"
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.common_stock_shares_outstanding
					  ) as "ranking"
				from
					temp_balance_sheet_parenthetical w1
		) e1
		where
			 e1.ranking = 1
	)
	,
	temp_is as(
		select
			 e1.*
			, e2.common_stock_outstanding
		from(
				select
					 w1.*
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.net_earnings_to_shareholder
					  ) as "ranking"
				from
					temp_income_statement w1
		) 					e1
		left join
			temp_bs_parent 	e2 on
				 e1.ticker_cik = e2.ticker_cik
				 	and
				 e1.date = e2.date
		where
			 e1.ranking = 1
	)
	,
	temp_bs as(
		select
			 e1.*
		from(
				select
					 w1.*
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.total_shareholder_equity
					  ) as "ranking"
				from
					temp_balance_sheet w1
		) e1
		where
			 e1.ranking = 1
	)
select
	 w1.ticker_cik
	, w1.table_name
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
		 w1.ticker_cik = w2.ticker_cik
		 	and
		 w1.date = w2.date
order by
	 w1.date
;
select
	 w1.*
from
	temp_bs w1
;

	
	
	
	
	
--select
--	 w1.*
--from
--	temp_balance_sheet w1
--;
--select
--	 w1.*
--from
--	temp_balance_sheet_parenthetical w1
--;
select
	 w1.*
from
	temp_income_statement w1
;