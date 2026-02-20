import os
import logging
from datetime import datetime, date
from supabase import create_client, Client

logger = logging.getLogger(__name__)

supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = None

if supabase_url and supabase_key:
    try:
        supabase = create_client(supabase_url, supabase_key)
        logger.info("✓ Connected to Supabase")
    except Exception as e:
        logger.error(f"Failed to connect to Supabase: {str(e)}")
else:
    logger.warning("⚠ Supabase credentials not configured.")


def execute_query(sql_query):
    """Execute a SQL query against Supabase and return results"""
    if not supabase:
        raise Exception("Database connection is not configured.")
    try:
        logger.info(f"Executing query: {sql_query}")
        result = supabase.rpc('exec_sql', {'query': sql_query}).execute()
        return result.data if result.data else []
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        raise Exception(f"Database error: {str(e)}")


def insert_time_off(slack_user_id, slack_user_name, first_day_off, last_day_off, original_message):
    """Insert a time-off record into the time_off table"""
    if not supabase:
        raise Exception("Database connection is not configured.")
    try:
        data = {
            'request_date': date.today().isoformat(),
            'slack_user_id': slack_user_id,
            'slack_user_name': slack_user_name,
            'first_day_off': first_day_off,
            'last_day_off': last_day_off,
            'original_message': original_message
        }
        logger.info(f"Inserting time_off: {data}")
        result = supabase.table('time_off').insert(data).execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to insert time off: {str(e)}")
        raise Exception(f"Database error: {str(e)}")


def lookup_user(slack_user_id):
    """Look up a user in yuri_user_directory by Slack ID.
    Returns dict with name, role, zoho_user_id, email — or None if not found."""
    if not supabase:
        logger.error("Database not configured — cannot look up user")
        return None
    try:
        result = supabase.table('yuri_user_directory') \
            .select('name, slack_user_id, email, zoho_user_id, role') \
            .eq('slack_user_id', slack_user_id) \
            .eq('is_active', True) \
            .execute()
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"User directory lookup failed: {str(e)}")
        return None


def fetch_rules():
    """Fetch all active rules from yuri_rules, ordered by sort_order.
    Returns a formatted string ready to inject into the system prompt."""
    if not supabase:
        logger.warning("Database not configured — cannot fetch rules")
        return ""
    try:
        result = supabase.table('yuri_rules') \
            .select('section, rule_key, rule_text') \
            .eq('is_active', True) \
            .order('sort_order') \
            .execute()
        if not result.data:
            return ""
        lines = []
        current_section = None
        for row in result.data:
            if row['section'] != current_section:
                current_section = row['section']
                header = current_section.upper().replace('_', ' ')
                lines.append(f"\n[{header}]")
            lines.append(f"- {row['rule_text']}")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Failed to fetch rules: {str(e)}")
        return ""


def get_schema_description():
    """Return database schema description for Claude"""
    return """
You have access to three areas of data: deals, deal_line_items, and time_off.
One deal can have multiple line items. Join them on zoho_deal_id.

═══════════════════════════════════════════════════════════
TABLE: deals (87 columns, one row per deal)
═══════════════════════════════════════════════════════════

IDENTIFICATION:
  zoho_deal_id (PK)          - Zoho Record Id
  sales_order_number         - SO number (e.g. '7158')
  deal_name                  - Full deal name
  account_name               - Customer / account
  parent_account             - Parent company
  contracted_so_number       - Contracted SO / MOT PO number
  customer_po_number         - Customer's PO number
  customer_po_number_required - Boolean
  pipeline                   - e.g. 'Order Pipeline'
  stage                      - e.g. 'Ready for Review', 'Shipped', 'Delivered and Paid', 'Unsigned'

OWNERSHIP & PEOPLE:
  deal_owner                 - Deal owner / sales rep
  sales_rep_on_account       - Rep assigned to account
  sales_rep_on_deal          - Rep assigned to deal
  po_signer                  - PO signer name
  created_by                 - Who created the deal
  deal_shared_with           - Shared with

FINANCIALS:
  amount                     - Deal amount (total)
  commission                 - Commission amount
  total_cost                 - Total cost
  total_deal_revenue         - Total revenue
  total_shipping_cost        - Shipping cost total
  gross_profit               - Gross profit
  gross_profit_margin        - Gross margin %
  net_profit                 - Net profit
  net_profit_margin          - Net margin %
  payment_terms              - e.g. 'Net 45', 'COD'
  pays_with_credit_card      - Boolean
  broker_1_commission        - Broker 1 commission (deal level)
  broker_2_commission        - Broker 2 commission (deal level)

DATES & TIMELINE:
  created_time               - Deal created (timestamp)
  modified_time              - Last modified (timestamp)
  closing_date               - Closing date
  contract_sent_date         - Contract sent (timestamp)
  deposit_invoice_sent_date  - Deposit invoice sent
  deposit_paid_date          - Deposit paid
  ideal_delivery_date        - Target delivery
  delivery_date              - Actual delivery date
  carrier_provided_eta       - Carrier ETA
  delivered_and_paid_date    - D&P date
  pickup_requested_date      - Pickup requested
  vtrust_requested_date      - V-TRUST requested
  vendor_invoice_request_date - Vendor invoice requested
  sent_to_quickbooks_date    - Sent to QB
  production_start_date      - Production start

STATUS & WORKFLOW:
  send_contract              - Boolean
  send_to_quickbooks         - Boolean
  qb_sync_status             - e.g. 'QB Sync Complete', 'Not started', 'Bill creation error'
  qb_sync_error              - Error message if sync failed
  pre_production_review_complete - Boolean
  proof_approved             - Boolean
  qc_report_approved         - Boolean
  quality_exception          - Boolean
  quality_exception_explanation - Text
  request_pickup             - Boolean
  request_vtrust_inspection  - Boolean
  request_vendor_invoice     - Boolean
  sample_sent                - Boolean
  packing_slip_sent_to_client - Boolean
  shared_deal                - Boolean
  stored                     - Boolean (local inventory)

SHIPPING & LOGISTICS:
  shipping_address           - Full address text
  shipping_method            - e.g. 'Air DDP', 'Speed Boat DDP'
  shipping_agent             - Shipping agent name
  incoterms                  - e.g. 'DDP'
  tracking_details           - Tracking info

VENDOR & LINKS:
  vendor                     - Vendor name
  vendor_po_number           - Vendor PO number
  vendor_po_link             - Link to vendor PO
  rfq_link                   - Link to RFQ
  signnow_po_upload_link     - SignNow upload link
  slack_thread_id            - Slack thread ID
  project_customer_id        - Customer project ID
  project_reference_id       - Reference project ID
  required_billing_ccs       - Billing CC emails
  required_po_ccs            - PO CC emails

METADATA & NOTES:
  pre_order_lead_source      - Lead source
  tag                        - Tags
  broker_notes               - Notes about broker
  description                - Deal description
  sample_contents            - Sample contents

AUDIT:
  dp_audit_status            - D&P audit status
  dp_audit_issue_description - D&P audit issue
  dp_audit_resolution_description - D&P audit resolution
  deal_audit_complete        - Deal audit complete
  shipped_audit_status       - Shipped audit status
  shipped_audit_issue_description - Shipped audit issue
  shipped_audit_resolution_description - Shipped audit resolution

═══════════════════════════════════════════════════════════
TABLE: deal_line_items (37 columns, one row per line item)
═══════════════════════════════════════════════════════════

IDENTIFICATION:
  zoho_line_item_id (PK)     - Zoho line item Record Id
  zoho_deal_id (FK → deals)  - Links to deals.zoho_deal_id

PRODUCT INFO:
  product_name               - Product name
  product_sku                - SKU code
  product_type               - e.g. 'Magnum', 'Z10', 'Filter Tip Booklets'
  product_category           - e.g. 'Plastic', 'Cartridges', 'Ancillary Products', 'Shipping Fee', 'Miscellaneous Fee'
  product_description        - Description text
  vendor_name                - Vendor for this line item
  quickbooks_item_id         - QB item ID
  quickbooks_account_id      - QB account ID

FINANCIALS:
  quantity                   - Units ordered
  unit_cost                  - Cost per unit (EXW or DDP)
  unit_price                 - Price per unit
  product_revenue            - Revenue for this line item
  shipping_cost              - Shipping cost for this line item
  cost_with_shipping         - Total cost including shipping
  broker_1_contact_name      - Broker 1 name
  broker_1_fee_per_unit      - Broker 1 per-unit fee
  broker_1_commission        - Broker 1 commission (line level)
  broker_2_contact_name      - Broker 2 name
  broker_2_fee_per_unit      - Broker 2 per-unit fee
  broker_2_commission        - Broker 2 commission (line level)

DATES & PRODUCTION:
  order_confirmation_date    - Order confirmed
  production_lead_time       - Lead time in days
  production_completion_date - Production done
  est_ship_date              - Estimated ship date
  actual_ship_date           - Actual ship date
  shipping_lead_time         - Shipping time in days
  auto_calculated_delivery_date - Calculated delivery
  actual_delivery_date       - Actual delivery
  created_time               - Line item created (timestamp)
  modified_time              - Line item modified (timestamp)

SHIPPING:
  shipping_solution          - Shipping method for this item
  tracking_number            - Tracking number
  incoterms                  - Incoterms for this item

QUALITY & NOTES:
  quality_control            - QC status
  remarks                    - Notes/remarks

═══════════════════════════════════════════════════════════
TABLE: time_off (tracks employee out-of-office dates)
═══════════════════════════════════════════════════════════

COLUMNS:
  id (PK)                    - Auto-incrementing ID
  request_date               - When the OOO was requested (DATE)
  slack_user_id              - Slack user ID (e.g. 'U0A8BSTE4SX')
  slack_user_name            - Employee name (e.g. 'Seth', 'Curley')
  first_day_off              - First day of time off (DATE)
  last_day_off               - Last day of time off (DATE)
  original_message           - Original Slack message text
  created_at                 - Record creation timestamp

COMMON QUERIES:
  - Who's out today:
    SELECT * FROM time_off WHERE CURRENT_DATE BETWEEN first_day_off AND last_day_off

  - Who's out this week:
    SELECT * FROM time_off
    WHERE first_day_off <= CURRENT_DATE + INTERVAL '7 days'
    AND last_day_off >= CURRENT_DATE

  - Is [person] out on [date]:
    SELECT * FROM time_off
    WHERE slack_user_name ILIKE '%name%'
    AND 'YYYY-MM-DD' BETWEEN first_day_off AND last_day_off

  - How many days has [person] taken off:
    SELECT slack_user_name,
           SUM(last_day_off - first_day_off + 1) as total_days
    FROM time_off
    WHERE slack_user_name ILIKE '%name%'
    GROUP BY slack_user_name

═══════════════════════════════════════════════════════════
TABLE: qbo_invoices (QuickBooks invoices — customer-facing)
═══════════════════════════════════════════════════════════

IDENTIFICATION:
  qbo_id (PK)                - QuickBooks invoice ID
  doc_number                 - Invoice number (e.g. '1042')
  txn_date                   - Invoice date
  due_date                   - Payment due date
  ship_date                  - Ship date

FINANCIALS:
  total_amt                  - Invoice total (in transaction currency)
  balance                    - Remaining balance in transaction currency (0 = fully paid)
  deposit                    - Deposit amount
  home_total_amt             - Invoice total in home currency (USD) — always populated
  home_balance               - Remaining balance in home currency (USD) — always populated
  currency_code              - e.g. 'USD', 'CAD'
  exchange_rate              - Exchange rate applied

CUSTOMER:
  customer_id                - QB customer ID
  customer_name              - Customer name
  bill_email                 - Billing email
  bill_addr_line1, bill_addr_city, bill_addr_state, bill_addr_postal_code, bill_addr_country
  ship_addr_line1, ship_addr_city, ship_addr_state, ship_addr_postal_code, ship_addr_country

STATUS:
  txn_status                 - Transaction status
  email_status               - Email delivery status
  print_status               - Print status

PAYMENT:
  sales_term_id / sales_term_name - Payment terms
  payment_method_id / payment_method_name - Payment method
  allow_online_payment       - Boolean
  allow_online_credit_card_payment - Boolean
  allow_online_ach_payment   - Boolean
  apply_tax_after_discount   - Boolean

SHIPPING:
  tracking_num               - Tracking number
  ship_method_name           - Shipping method

OTHER:
  private_note               - Internal note
  customer_memo              - Customer-facing memo
  linked_txns                - JSONB linked transactions
  line_items                 - JSONB invoice line items
  created_time               - Record created (timestamp)
  last_updated_time          - Last modified (timestamp)
  synced_at                  - Last sync from QB (timestamp)

COMMON QUERIES:
  - Outstanding invoices: SELECT * FROM qbo_invoices WHERE balance > 0
  - Past-due invoices: SELECT * FROM qbo_invoices WHERE balance > 0 AND due_date < CURRENT_DATE
  - Fully paid: SELECT * FROM qbo_invoices WHERE balance = 0
  - Total receivables: SELECT SUM(home_balance) FROM qbo_invoices WHERE balance > 0

═══════════════════════════════════════════════════════════
TABLE: qbo_bills (QuickBooks bills — vendor/supplier-facing)
═══════════════════════════════════════════════════════════

IDENTIFICATION:
  qbo_id (PK)                - QuickBooks bill ID
  doc_number                 - Bill number
  txn_date                   - Bill date
  due_date                   - Payment due date

FINANCIALS:
  total_amt                  - Bill total (in transaction currency)
  balance                    - Remaining balance in transaction currency (0 = fully paid)
  home_total_amt             - Bill total in home currency (USD) — always populated
  home_balance               - Remaining balance in home currency (USD) — always populated
  currency_code              - e.g. 'USD', 'CNY'
  exchange_rate              - Exchange rate applied

VENDOR:
  vendor_id                  - QB vendor ID
  vendor_name                - Vendor name

ACCOUNTS:
  ap_account_id              - AP account ID
  ap_account_name            - AP account name

OTHER:
  private_note               - Internal note
  linked_txns                - JSONB linked transactions
  line_items                 - JSONB bill line items
  created_time               - Record created (timestamp)
  last_updated_time          - Last modified (timestamp)
  synced_at                  - Last sync from QB (timestamp)

COMMON QUERIES:
  - Outstanding bills: SELECT * FROM qbo_bills WHERE balance > 0
  - Past-due bills: SELECT * FROM qbo_bills WHERE balance > 0 AND due_date < CURRENT_DATE
  - Bills by vendor: SELECT * FROM qbo_bills WHERE vendor_name ILIKE '%vendor%'
  - Total payables: SELECT SUM(home_balance) FROM qbo_bills WHERE balance > 0

═══════════════════════════════════════════════════════════
TABLE: qbo_estimates (QuickBooks estimates/quotes)
═══════════════════════════════════════════════════════════

IDENTIFICATION:
  qbo_id (PK)                - QuickBooks estimate ID
  doc_number                 - Estimate number
  txn_date                   - Estimate date
  expiration_date            - Expiration date

FINANCIALS:
  total_amt                  - Estimate total (in transaction currency)
  home_total_amt             - Estimate total in home currency (USD) — always populated
  currency_code              - e.g. 'USD'
  exchange_rate              - Exchange rate applied

CUSTOMER:
  customer_id                - QB customer ID
  customer_name              - Customer name
  bill_email                 - Billing email
  bill_addr_line1, bill_addr_city, bill_addr_state, bill_addr_postal_code, bill_addr_country
  ship_addr_line1, ship_addr_city, ship_addr_state, ship_addr_postal_code, ship_addr_country

STATUS:
  txn_status                 - e.g. 'Accepted', 'Pending', 'Closed'
  email_status               - Email delivery status
  print_status               - Print status
  accepted_by                - Who accepted
  accepted_date              - Date accepted

TERMS:
  sales_term_id / sales_term_name - Sales terms

OTHER:
  private_note               - Internal note
  customer_memo              - Customer-facing memo
  linked_txns                - JSONB linked transactions
  line_items                 - JSONB estimate line items
  created_time               - Record created (timestamp)
  last_updated_time          - Last modified (timestamp)
  synced_at                  - Last sync from QB (timestamp)

═══════════════════════════════════════════════════════════
QUERY GUIDELINES
═══════════════════════════════════════════════════════════

- To get deal info only: SELECT from deals
- To get line items for a deal: SELECT from deal_line_items WHERE zoho_deal_id = '...'
- To get full deal + items: JOIN deals d ON d.zoho_deal_id = li.zoho_deal_id
- Use ILIKE for case-insensitive text searches
- sales_order_number is TEXT, not integer (e.g. '7158')
- Dates: DATE columns are YYYY-MM-DD, TIMESTAMPTZ columns include time
- Financial columns are NUMERIC
- Boolean columns are true/false
- When asked about products, quantities, or line-item details, always query deal_line_items
- When asked about deal status, financials, or ownership, query deals
- When asked about a specific SO number, join both tables for complete info
- Avoid SELECT * on joins — pick specific columns to keep responses readable
- When asked about time off, OOO, who's out, vacation, PTO — query time_off table

QB DATA GUIDELINES:
- When asked about invoices, receivables, or customer billing — query qbo_invoices
- When asked about bills, payables, or vendor payments — query qbo_bills
- When asked about estimates or quotes — query qbo_estimates
- For amounts in USD, use home_total_amt and home_balance (these are always populated)
- For filtering paid/unpaid status, use balance (not home_balance) for the boolean check:
    "Past due" = balance > 0 AND due_date < CURRENT_DATE
    "Outstanding" / "unpaid" = balance > 0
    "Paid" = balance = 0
- For dollar amounts in results, display home_total_amt and home_balance (USD values)
- Do NOT confuse deals data with QB data — deals track the sales pipeline, QB tables track actual invoices/bills/estimates

IMPORTANT — QB TABLES ARE STANDALONE:
- qbo_invoices, qbo_bills, and qbo_estimates have NO foreign key to deals
- Customer names in QB do NOT match account_name in deals (different naming conventions)
- NEVER join QB tables to deals — it will silently return zero rows
- When asked about invoices "by sales rep" or "by account," group by customer_name instead
- QB data stands on its own — query it directly from the qbo_ tables

═══════════════════════════════════════════════════════════
TABLE: yuri_user_directory (maps Slack users to Zoho IDs and roles)
═══════════════════════════════════════════════════════════

COLUMNS:
  id (PK)                    - Auto-incrementing ID
  name                       - Employee name
  slack_user_id              - Slack user ID (e.g. 'U0A8BSTE4SX')
  email                      - Email address
  zoho_user_id               - Zoho CRM user ID
  role                       - 'admin' or 'standard'
  is_active                  - Boolean

═══════════════════════════════════════════════════════════
TABLE: yuri_broker_lookup (resolves Broker ID codes to names — ADMIN ONLY)
═══════════════════════════════════════════════════════════

COLUMNS:
  id (PK)                    - Auto-incrementing ID
  broker_id                  - Broker code (e.g. 'KGM77')
  first_name                 - Broker first name
  last_name                  - Broker last name
  is_active                  - Boolean

NOTE: Only resolve broker IDs for admin users. Standard users must only see the broker_id code.
"""
