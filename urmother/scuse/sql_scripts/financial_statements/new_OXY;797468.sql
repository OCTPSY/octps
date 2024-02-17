
--=====================================================
--	COMPANY NAME : OCCIDENTAL PETROLEUM CORP
--	CENTRAL INDEX KEY : 797468
--=====================================================

with
	temp_balance_sheet as(
		--UPDATE HERE
select 'OXY;797468' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_10921022671322' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash_and_restricted_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "trade_receivables_net_of_reserves_of_35_in_2021_and_24_in_2020", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "inventories", 0 ) as float ) ) / 1 ) as "inventories", ( null ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "total_property_plant_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease_assets", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "long_term_debt_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_0_20_per_share_par_value_authorized_shares_1_5_billion_issued_shares_2021_1083423094_and_2020_1080564947", 0 ) as float ) + cast( coalesce( "additional_paid_in_capital", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_ 2021_149348394_shares_and_2020_149051634_shares", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) - cast( coalesce( "preferred_stock_at_1_00_per_share_par_value_100000_shares_as_of_december_31_2021_and_2020", 0 ) as float ) ) / 1 
) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_10921022671322" union all select 'OXY;797468' as "ticker_cik", '10_K2020_02_28_CONSOLIDATED_BALANCE_SHEETS_10921020665475' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash_and_restricted_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "trade_receivables_net_of_reserves_of_19_in_2019_and_21_in_2018", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( ( 
 + cast( coalesce( "inventories", 0 ) as float ) ) / 1 ) as "inventories", ( null ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "property_plant_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease_assets", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  
+ cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 
0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "long_term_debt_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_0_20_per_share_par_value_authorized_shares_1_1_billion_issued_shares_2019_1044434893_and_2018_895115637", 0 ) as float ) + cast( coalesce( "additional_paid_in_capital", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_2019_150323151_shares_and_2018_145726051_shares", 0 ) as float ) ) / 1 ) 
as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) - cast( coalesce( "preferred_stock_at_1_00_per_share_par_value_100000_shares_at_december_31_2019", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2020_02_28_CONSOLIDATED_BALANCE_SHEETS_10921020665475" union all select 'OXY;797468' as "ticker_cik", '10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_10921018637718' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "trade_receivables_net_of_reserves_of_16_in_2017_and_16_in_2016", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "inventories", 0 ) as float ) ) / 1 ) as "inventories", ( null ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "property_plant_and_equipment_net", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) ) / 1 ) as "short_term_debt", ( 
null ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt_net", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_debt_net", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock_0_20_per_share_par_value_authorized_shares_1_1_billion_issued_shares_2017_893468707_and_2016_892214604", 0 ) as float ) + cast( coalesce( "additional_paid_in_capital", 0 ) as float ) ) / 
1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock_2017_128364195_shares_and_2016_127977306_shares", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  
+ cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_10921018637718"
	)
	,
	temp_balance_sheet_parenthetical as(
		--UPDATE HERE
select 'OXY;797468' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921022671322' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_issued_shares_in_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_shares_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921022671322" union all select 'OXY;797468' as "ticker_cik", '10_K2020_02_28_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921020665475' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_issued_shares_in_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_shares_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2020_02_28_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921020665475" union all select 'OXY;797468' as "ticker_cik", '10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921018637718' as "table_name", date as "date",  ( 
null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null 
) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_issued_shares", 0 ) as float ) - cast( coalesce( "treasury_stock_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10921018637718"
	)
	,
	temp_income_statement as(
		--UPDATE HERE
select 'OXY;797468' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921022671322' as "table_name", date as "date",  ( (  + cast( coalesce( "net_sales", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "oil_and_gas_operating_expense", 0 ) as float ) - cast( coalesce( "transportation_and_gathering_expense", 0 ) as float ) - cast( coalesce( "chemical_and_midstream_cost_of_sales", 0 ) as float ) - cast( coalesce( "purchased_commodities", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "selling_general_and_administrative", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_depletion_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "income_loss_before_income_taxes_and_other_items", 0 ) as float ) + cast( coalesce( "interest_and_debt_expense_net", 0 ) as float ) ) / 1 ) as "operating_income", ( null ) as "interest_expense", ( (  - cast( coalesce( "interest_and_debt_expense_net", 0 ) as float ) ) / 1 ) as "interest_expense, net", ( (  + cast( coalesce( "income_tax_benefit_expense", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "income_loss_from_continuing_operations", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( (  + cast( coalesce( "loss_from_discontinued_operations_net_of_tax", 0 ) as float ) ) / 1 ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income_loss", 0 ) as 
float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2022_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921022671322" union all select 'OXY;797468' as "ticker_cik", '10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921020665475' as "table_name", date as "date",  ( (  + cast( coalesce( "net_sales", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "oil_and_gas_operating_expense", 0 ) as float ) - cast( coalesce( "transportation_expense", 0 ) as float ) - cast( coalesce( "chemical_and_midstream_cost_of_sales", 0 ) as float ) - cast( coalesce( "purchased_commodities", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "selling_general_and_administrative", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_depletion_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "income_loss_before_income_taxes_and_other_items", 0 ) as float ) + cast( coalesce( "interest_and_debt_expense_net", 0 ) as float ) ) / 1 ) as "operating_income", ( null ) as "interest_expense", ( (  - cast( coalesce( "interest_and_debt_expense_net", 0 ) as float ) ) / 1 ) as "interest_expense, net", ( (  + cast( coalesce( "income_tax_expense", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "income_loss_from_continuing_operations", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( (  + cast( coalesce( "loss_from_discontinued_operations_net_of_tax", 0 ) as float ) ) / 1 ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income_loss_attributable_to_common_stockholders", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2020_02_28_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921020665475" union all select 'OXY;797468' as "ticker_cik", '10_K2018_02_23_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921018637718' as "table_name", date as "date",  ( (  + cast( coalesce( "net_sales", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( 
"cost_of_sales_excludes_depreciation_depletion_and_amortization_of_4000_in_2017_4266_in_2016_and_4540_in_2015", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "selling_general_and_administrative_and_other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_depletion_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "income_loss_before_income_taxes_and_other_items", 0 ) as float ) + cast( coalesce( "interest_and_debt_expense_net", 0 ) as float ) ) / 1 ) as "operating_income", ( null ) as "interest_expense", ( (  - cast( coalesce( "interest_and_debt_expense_net", 0 ) 
as float ) ) / 1 ) as "interest_expense, net", ( (  + cast( coalesce( "_provision_for_benefit_from_domestic_and_foreign_income_taxes", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "income_loss_from_continuing_operations", 0 ) as float ) ) / 1 
) as "continuing_net_earnings_total", ( (  + cast( coalesce( "income_from_discontinued_operations", 0 ) as float ) ) / 1 ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income_loss", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2018_02_23_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_10921018637718"
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