import os
import re
import time
import logging
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from anthropic import Anthropic
from database import execute_query, get_schema_description, insert_time_off, lookup_user, fetch_rules
from calendar_service import create_ooo_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
anthropic = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

OOO_CHANNEL_ID = "C0AF0CJL42F"
BOT_USER_ID = None

# =============================================================================
# CONVERSATION MEMORY
# =============================================================================

conversation_store = {}
MAX_TURNS = 10
HISTORY_TTL_MINUTES = 30


def get_conversation_key(channel, channel_type, user_id):
    if channel_type == "im":
        return f"dm_{user_id}"
    return f"ch_{channel}_{user_id}"


def get_history(key):
    if key not in conversation_store:
        return []
    entry = conversation_store[key]
    if datetime.now() - entry["last_activity"] > timedelta(minutes=HISTORY_TTL_MINUTES):
        del conversation_store[key]
        return []
    return list(entry["messages"])


def save_history(key, messages):
    trimmed = trim_history(messages, MAX_TURNS)
    conversation_store[key] = {
        "messages": trimmed,
        "last_activity": datetime.now()
    }


def trim_history(messages, max_turns):
    turn_starts = []
    for i, msg in enumerate(messages):
        if msg["role"] != "user":
            continue
        content = msg["content"]
        if isinstance(content, list) and any(
            isinstance(c, dict) and c.get("type") == "tool_result"
            for c in content
        ):
            continue
        turn_starts.append(i)
    if len(turn_starts) > max_turns:
        start_idx = turn_starts[-max_turns]
        return messages[start_idx:]
    return messages


def cleanup_stale_conversations():
    now = datetime.now()
    stale = [
        k for k, v in conversation_store.items()
        if now - v["last_activity"] > timedelta(minutes=HISTORY_TTL_MINUTES * 2)
    ]
    for k in stale:
        del conversation_store[k]


# =============================================================================
# RULES CACHE
# =============================================================================

_rules_cache = {"text": "", "fetched_at": None}
RULES_CACHE_TTL_MINUTES = 15


def get_cached_rules():
    """Fetch rules from yuri_rules table, cached for 15 minutes."""
    now = datetime.now()
    if _rules_cache["fetched_at"] and (now - _rules_cache["fetched_at"]) < timedelta(minutes=RULES_CACHE_TTL_MINUTES):
        return _rules_cache["text"]
    rules_text = fetch_rules()
    _rules_cache["text"] = rules_text
    _rules_cache["fetched_at"] = now
    if rules_text:
        logger.info(f"Rules cache refreshed ({len(rules_text)} chars)")
    else:
        logger.warning("Rules cache refresh returned empty")
    return rules_text


# =============================================================================
# SYSTEM PROMPTS
# =============================================================================

BUSINESS_CONTEXT = """
COMPANY CONTEXT:
- Finished Goods is a product sourcing and procurement company (~15 employees)
- "SO" or "Sales Order" numbers are the primary way people reference deals (e.g. "SO 7158" or just "7158")
- "D&P" is shorthand for "Delivered and Paid"
- Deals can also be "Closed Lost" (lost/cancelled)
- Each deal can have multiple line items (different products in the same order)
- Line items have their own production and shipping timelines independent of each other
- Vendors are the suppliers/manufacturers (mostly in China)
- Brokers earn commissions on deals they facilitate
- V-TRUST is a third-party quality inspection service
- Incoterms like "DDP" (Delivered Duty Paid) and "EXW" (Ex Works) describe shipping cost responsibility
- QuickBooks (QB) is used for invoicing and accounting

RESPONSE GUIDELINES:
- Be concise but thorough — give the key facts without unnecessary filler
- When showing deal info, lead with stage, customer, and amount
- When showing multiple deals, use a clean format but don't over-format
- If a question is ambiguous, make your best guess AND ask for clarification
- If results are empty, suggest what they might try instead
- When a user refers to something from earlier in the conversation, use context — don't ask them to repeat themselves
- FORMATTING: You are responding in Slack, which uses *single asterisks* for bold (NOT **double**). Use Slack mrkdwn syntax: *bold*, _italic_, ~strikethrough~, `code`. Do NOT use Markdown-style **bold** or ### headers.
"""

SYSTEM_PROMPT = """You are Yuri, a helpful data assistant for the company Finished Goods.

Today's date is {today} ({day_of_week}).

{business_context}

═══════════════════════════════════════════════════════════
CURRENT USER
═══════════════════════════════════════════════════════════
{user_context}

═══════════════════════════════════════════════════════════
BOT RULES (from yuri_rules — always follow these)
═══════════════════════════════════════════════════════════
{rules}

═══════════════════════════════════════════════════════════
DATABASE SCHEMA
═══════════════════════════════════════════════════════════
{schema}

When users ask questions about data, you should:
1. Look up the current user's role and apply the appropriate data access rules
2. Generate a SQL query to fetch the relevant information (with proper access filtering)
3. Use the execute_sql tool to run the query
4. Provide a clear, conversational answer based on the results

CRITICAL — DATA ACCESS ENFORCEMENT:
- For ADMIN users: No deal filtering needed. They can see all data.
- For STANDARD users: ALWAYS add a WHERE clause that filters deals to only those where the user's name
  appears in deal_owner, sales_rep_on_account, or sales_rep_on_deal. Use the user's name from the
  CURRENT USER section above. Example:
  WHERE (deal_owner ILIKE '%Ann%' OR sales_rep_on_account ILIKE '%Ann%' OR sales_rep_on_deal ILIKE '%Ann%')
- If a standard user asks for data that isn't scoped to them, politely explain they can only see their own deals.
- NEVER skip this filtering for standard users, even if they ask to see "all" deals.

Be helpful, friendly, and concise. If you're unsure about something, ask for clarification.
If a query returns no results, explain that clearly and suggest alternatives.

Important: Always use proper SQL syntax for Postgres. Use single quotes for string literals.

When responding to users, use their name naturally in conversation to make interactions more personal.

When referencing dates from query results, always calculate the exact difference from today's date.
Do NOT say "yesterday" unless the date is exactly 1 day before today, "last week" unless it was actually 7+ days ago, etc. Be precise with relative dates.

CONVERSATION CONTEXT:
You may see previous messages in this conversation. Use them to understand follow-up questions.
If the user says things like "yes", "check that", "show me more", "what about line items", etc.,
refer to the prior context to understand what they're asking about. Never ask them to repeat
information that's already in the conversation history.

TIME-OFF PRIVACY RULES:
- Users CAN ask "who's out today?" or "who's out this week?" — that's scheduling info everyone needs.
- Users can ONLY check their OWN time-off totals (e.g., "how many days have I taken off this year?").
- If a user asks about ANOTHER person's total days off, accumulated PTO, or time-off history, politely decline and let them know they can only view their own totals.
- To identify the requesting user, match their Slack ID (provided in the message context) against the slack_user_id column.
"""

OOO_SYSTEM_PROMPT = """You are Yuri, a helpful assistant for the company Finished Goods.

You are monitoring the #out-of-office Slack channel. When employees post about taking time off, your job is to:
1. Parse the dates from their message
2. Record it in the time_off database using the insert_time_off tool
3. Respond with a brief, friendly confirmation

The person posting the message is the one taking time off, unless they explicitly say otherwise (e.g., "Seth will be out Friday").

Today's date is {today} ({day_of_week}).

THIS WEEK'S DATES (for reference):
{week_dates}

PARSING RULES:
- Extract first_day_off and last_day_off as YYYY-MM-DD format
- If only one date is mentioned, first_day_off and last_day_off are the same
- If a range is given (e.g., "March 15-17" or "Monday through Wednesday"), use the start and end dates
- Handle relative dates: "next Monday", "this Friday", "tomorrow", etc. based on today's date
- USE THE WEEK DATES ABOVE to map day names to exact dates — do NOT calculate dates yourself
- Handle partial ranges like "out the 15th through the 17th" — infer the month from context
- If the year isn't specified, assume the current year (or next year if the date has clearly passed)
- If the message is unclear or you can't confidently parse dates, ask for clarification politely
- If the message is NOT an OOO request (e.g., a casual reply, "thanks", a question, a query about who's out, how many days taken, etc.), respond briefly telling them to DM you or mention you in another channel for questions. Example: "This channel is just for posting time-off requests — DM me or @Yuri me in another channel to check who's out or look up time-off info!"
- Do NOT use the insert_time_off tool for non-OOO messages.

IMPORTANT: Only use the insert_time_off tool when you're confident this is a legitimate time-off announcement with parseable dates.

RESPONSE STYLE:
- Keep confirmations brief and consistent. Use this format:
  Single day: "Time off has been scheduled for [YYYY-MM-DD] and added to the tracker and calendar."
  Date range: "Time off has been scheduled from [YYYY-MM-DD] to [YYYY-MM-DD] and added to the tracker and calendar."
- If the tool result shows calendar_event_created is false, say "added to the tracker" (without mentioning calendar).
- Don't be overly verbose. One sentence for confirmations.
"""

# =============================================================================
# TOOLS
# =============================================================================

query_tools = [
    {
        "name": "execute_sql",
        "description": "Execute a SQL query against the Supabase database and return the results. Use this when you need to fetch data to answer a user's question.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL query to execute. Must be a valid Postgres SQL query."
                },
                "explanation": {
                    "type": "string",
                    "description": "A brief explanation of what this query does, for logging purposes."
                }
            },
            "required": ["query", "explanation"]
        }
    }
]

ooo_tools = [
    {
        "name": "insert_time_off",
        "description": "Insert a new time-off record into the database. Use this when an employee posts about taking time off in the #out-of-office channel.",
        "input_schema": {
            "type": "object",
            "properties": {
                "slack_user_id": {
                    "type": "string",
                    "description": "The Slack user ID of the person taking time off"
                },
                "slack_user_name": {
                    "type": "string",
                    "description": "The display name of the person taking time off"
                },
                "first_day_off": {
                    "type": "string",
                    "description": "First day off in YYYY-MM-DD format"
                },
                "last_day_off": {
                    "type": "string",
                    "description": "Last day off in YYYY-MM-DD format"
                },
                "original_message": {
                    "type": "string",
                    "description": "The original Slack message text"
                }
            },
            "required": ["slack_user_id", "slack_user_name", "first_day_off", "last_day_off", "original_message"]
        }
    }
]

# =============================================================================
# TOOL PROCESSORS
# =============================================================================

def process_tool_call(tool_name, tool_input):
    if tool_name == "execute_sql":
        query = tool_input["query"]
        explanation = tool_input.get("explanation", "No explanation provided")
        logger.info(f"Executing SQL query: {explanation}")
        logger.info(f"Query: {query}")
        try:
            results = execute_query(query)
            return {
                "success": True,
                "results": results,
                "row_count": len(results) if results else 0
            }
        except Exception as e:
            logger.error(f"SQL query failed: {str(e)}")
            return {"success": False, "error": str(e)}

    elif tool_name == "insert_time_off":
        logger.info(f"Inserting time off for {tool_input['slack_user_name']}: {tool_input['first_day_off']} to {tool_input['last_day_off']}")
        try:
            result = insert_time_off(
                slack_user_id=tool_input["slack_user_id"],
                slack_user_name=tool_input["slack_user_name"],
                first_day_off=tool_input["first_day_off"],
                last_day_off=tool_input["last_day_off"],
                original_message=tool_input["original_message"]
            )
            cal_result = create_ooo_event(
                user_name=tool_input["slack_user_name"],
                first_day_off=tool_input["first_day_off"],
                last_day_off=tool_input["last_day_off"]
            )
            if cal_result["success"]:
                logger.info(f"Calendar event created for {tool_input['slack_user_name']}")
            else:
                logger.warning(f"Calendar event skipped: {cal_result.get('error', 'unknown')}")
            return {
                "success": True,
                "message": f"Time off recorded for {tool_input['slack_user_name']}",
                "calendar_event_created": cal_result["success"]
            }
        except Exception as e:
            logger.error(f"Failed to insert time off: {str(e)}")
            return {"success": False, "error": str(e)}

    return {"success": False, "error": "Unknown tool"}

# =============================================================================
# HELPERS
# =============================================================================

def get_week_dates():
    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    this_monday = today - timedelta(days=today.weekday())
    lines = []
    for i in range(14):
        d = this_monday + timedelta(days=i)
        prefix = " <-- TODAY" if d == today else ""
        lines.append(f"  {d.strftime('%A')}: {d.strftime('%Y-%m-%d')}{prefix}")
        if i == 6:
            lines.append("  ---")
    return "\n".join(lines)


def build_user_context(slack_user_id, slack_name):
    """Look up user in yuri_user_directory and build context string for the system prompt."""
    directory_user = lookup_user(slack_user_id)
    if directory_user:
        role = directory_user['role']
        name = directory_user['name']
        zoho_id = directory_user.get('zoho_user_id', 'unknown')
        if role == 'admin':
            return (
                f"Name: {name}\n"
                f"Slack ID: {slack_user_id}\n"
                f"Zoho ID: {zoho_id}\n"
                f"Role: ADMIN — full access to all deals, accounts, and systems. No deal filtering required."
            )
        else:
            return (
                f"Name: {name}\n"
                f"Slack ID: {slack_user_id}\n"
                f"Zoho ID: {zoho_id}\n"
                f"Role: STANDARD — can only access deals where '{name}' appears in deal_owner, sales_rep_on_account, or sales_rep_on_deal.\n"
                f"All SQL queries for deal data MUST include a WHERE clause filtering on these columns using this user's name."
            )
    else:
        return (
            f"Name: {slack_name}\n"
            f"Slack ID: {slack_user_id}\n"
            f"Role: UNKNOWN — user not found in yuri_user_directory. Do NOT return any deal data.\n"
            f"Politely tell them to contact an administrator to get set up."
        )


def get_user_info(client, user_id):
    try:
        result = client.users_info(user=user_id)
        user = result["user"]
        real_name = user.get("real_name", "")
        display_name = user.get("profile", {}).get("display_name", "")
        email = user.get("profile", {}).get("email", "")
        name = display_name if display_name else real_name
        return {"name": name, "email": email, "slack_id": user_id}
    except Exception as e:
        logger.error(f"Failed to get user info: {str(e)}")
        return {"name": "there", "email": "", "slack_id": user_id}


def classify_api_error(e):
    """Classify an Anthropic API error and return an appropriate user-facing message."""
    error_str = str(e).lower()
    if "rate_limit" in error_str or "429" in error_str:
        return "⚠️ I'm being rate limited by my AI provider. Give me about 60 seconds and try again — I can definitely answer this!"
    elif "overloaded" in error_str or "529" in error_str:
        return "⚠️ My AI provider's servers are at capacity right now. This is temporary — try again in a minute or two!"
    else:
        return "Sorry, I encountered an error processing your request. Please try again."


def format_for_slack(text):
    """Convert Markdown formatting to Slack mrkdwn formatting."""
    # **bold** → *bold*
    text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', text)
    # Remove any remaining markdown headers (### Header → Header)
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    return text


def chat_with_claude(user_message, user_info, system_prompt, tools_list, history=None):
    """Send a message to Claude with conversation history and handle tool calls"""
    contextual_message = f"User: {user_info['name']}"
    if user_info.get('email'):
        contextual_message += f" (email: {user_info['email']})"
    contextual_message += f"\nSlack ID: {user_info['slack_id']}\n\nMessage: {user_message}"

    if history:
        messages = list(history)
    else:
        messages = []

    messages.append({"role": "user", "content": contextual_message})

    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=system_prompt,
        tools=tools_list,
        messages=messages
    )

    while response.stop_reason == "tool_use":
        tool_results = []
        assistant_content = []

        for block in response.content:
            if block.type == "tool_use":
                result = process_tool_call(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": str(result)
                })
                assistant_content.append(block)
            elif block.type == "text":
                assistant_content.append(block)

        messages.append({"role": "assistant", "content": assistant_content})
        messages.append({"role": "user", "content": tool_results})

        response = anthropic.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=system_prompt,
            tools=tools_list,
            messages=messages
        )

    # Append final assistant response to history
    messages.append({"role": "assistant", "content": response.content})

    final_response = ""
    for block in response.content:
        if block.type == "text":
            final_response += block.text

    return format_for_slack(final_response), messages


# =============================================================================
# STREAMING
# =============================================================================

SLACK_UPDATE_INTERVAL = 1.5  # seconds between Slack message edits


def post_placeholder(client, channel, thread_ts=None):
    """Post ⏳ placeholder and return its timestamp for later editing."""
    kwargs = {"channel": channel, "text": "⏳"}
    if thread_ts:
        kwargs["thread_ts"] = thread_ts
    result = client.chat_postMessage(**kwargs)
    return result["ts"]


def chat_with_claude_streaming(user_message, user_info, system_prompt, tools_list,
                                client, channel, message_ts, history=None):
    """Send a message to Claude, stream the final response to Slack by editing message_ts."""
    contextual_message = f"User: {user_info['name']}"
    if user_info.get('email'):
        contextual_message += f" (email: {user_info['email']})"
    contextual_message += f"\nSlack ID: {user_info['slack_id']}\n\nMessage: {user_message}"

    if history:
        messages = list(history)
    else:
        messages = []

    messages.append({"role": "user", "content": contextual_message})

    first_update = True

    while True:
        accumulated_text = ""
        last_update_time = 0

        with anthropic.messages.stream(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=system_prompt,
            tools=tools_list,
            messages=messages
        ) as stream:
            for text in stream.text_stream:
                accumulated_text += text
                now = time.time()
                # First update: wait for 40+ chars so we don't flash "I" or "I'll"
                # Subsequent updates: normal interval
                min_length = 40 if first_update else 0
                if (now - last_update_time >= SLACK_UPDATE_INTERVAL
                        and len(accumulated_text.strip()) >= min_length):
                    try:
                        client.chat_update(channel=channel, ts=message_ts, text=format_for_slack(accumulated_text))
                        first_update = False
                    except Exception:
                        pass
                    last_update_time = now

            response = stream.get_final_message()

        if response.stop_reason == "tool_use":
            tool_results = []
            assistant_content = []

            for block in response.content:
                if block.type == "tool_use":
                    result = process_tool_call(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": str(result)
                    })
                    assistant_content.append(block)
                elif block.type == "text":
                    assistant_content.append(block)

            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})
            continue
        else:
            # Final response — one last Slack update with complete text
            final_text = ""
            for block in response.content:
                if block.type == "text":
                    final_text += block.text

            try:
                client.chat_update(channel=channel, ts=message_ts, text=format_for_slack(final_text))
            except Exception as e:
                logger.error(f"Failed final Slack update: {e}")

            messages.append({"role": "assistant", "content": response.content})
            return final_text, messages

# =============================================================================
# EVENT HANDLERS
# =============================================================================

@app.event("app_mention")
def handle_mention(event, say, client, logger):
    user_id = event["user"]
    text = event["text"]
    channel = event.get("channel", "")

    text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

    if not text:
        say("Hi! How can I help you today?")
        return

    logger.info(f"Received mention from user {user_id} in channel {channel}: {text}")

    user_info = get_user_info(client, user_id)

    # OOO channel — no conversation memory needed
    if channel == OOO_CHANNEL_ID:
        today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        system_prompt = OOO_SYSTEM_PROMPT.format(
            today=today.strftime("%Y-%m-%d"),
            day_of_week=today.strftime("%A"),
            week_dates=get_week_dates()
        )
        try:
            response, _ = chat_with_claude(text, user_info, system_prompt, ooo_tools)
            say(text=response, thread_ts=event["ts"])
        except Exception as e:
            logger.error(f"Error processing OOO mention: {str(e)}")
            say(text="Sorry, I had trouble processing that. Could you try rephrasing your time-off request?", thread_ts=event["ts"])
        return

    # All other channels — with conversation memory
    conv_key = get_conversation_key(channel, "channel", user_id)
    history = get_history(conv_key)

    schema_desc = get_schema_description()
    rules_text = get_cached_rules()
    user_context = build_user_context(user_id, user_info['name'])
    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    system_prompt = SYSTEM_PROMPT.format(
        schema=schema_desc,
        business_context=BUSINESS_CONTEXT,
        rules=rules_text,
        user_context=user_context,
        today=today.strftime("%Y-%m-%d"),
        day_of_week=today.strftime("%A")
    )

    try:
        placeholder_ts = post_placeholder(client, channel)
        response, updated_history = chat_with_claude_streaming(
            text, user_info, system_prompt, query_tools,
            client, channel, placeholder_ts, history
        )
        save_history(conv_key, updated_history)
    except Exception as e:
        logger.error(f"Error processing mention: {str(e)}")
        error_msg = classify_api_error(e)
        try:
            client.chat_update(channel=channel, ts=placeholder_ts, text=error_msg)
        except Exception:
            say(error_msg)

    cleanup_stale_conversations()


@app.event("message")
def handle_message(event, say, client, logger):
    if event.get("bot_id") or event.get("subtype"):
        return

    user_id = event.get("user")
    text = event.get("text", "")
    channel = event.get("channel", "")
    channel_type = event.get("channel_type", "")

    if not text or not user_id:
        return

    # OOO channel — no conversation memory
    if channel == OOO_CHANNEL_ID:
        if BOT_USER_ID and f"<@{BOT_USER_ID}>" in text:
            return

        logger.info(f"OOO message from user {user_id}: {text}")
        user_info = get_user_info(client, user_id)

        today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        system_prompt = OOO_SYSTEM_PROMPT.format(
            today=today.strftime("%Y-%m-%d"),
            day_of_week=today.strftime("%A"),
            week_dates=get_week_dates()
        )
        try:
            response, _ = chat_with_claude(text, user_info, system_prompt, ooo_tools)
            say(text=response, thread_ts=event["ts"])
        except Exception as e:
            logger.error(f"Error processing OOO message: {str(e)}")
            say(text="Sorry, I had trouble processing that. Could you try rephrasing your time-off request?", thread_ts=event["ts"])
        return

    # DMs — with conversation memory
    if channel_type == "im":
        logger.info(f"Received DM from user {user_id}: {text}")

        user_info = get_user_info(client, user_id)
        conv_key = get_conversation_key(channel, channel_type, user_id)
        history = get_history(conv_key)

        schema_desc = get_schema_description()
        rules_text = get_cached_rules()
        user_context = build_user_context(user_id, user_info['name'])
        today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        system_prompt = SYSTEM_PROMPT.format(
            schema=schema_desc,
            business_context=BUSINESS_CONTEXT,
            rules=rules_text,
            user_context=user_context,
            today=today.strftime("%Y-%m-%d"),
            day_of_week=today.strftime("%A")
        )

        try:
            placeholder_ts = post_placeholder(client, channel)
            response, updated_history = chat_with_claude_streaming(
                text, user_info, system_prompt, query_tools,
                client, channel, placeholder_ts, history
            )
            save_history(conv_key, updated_history)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            error_msg = classify_api_error(e)
            try:
                client.chat_update(channel=channel, ts=placeholder_ts, text=error_msg)
            except Exception:
                say(error_msg)

        cleanup_stale_conversations()
        return

# =============================================================================
# STARTUP
# =============================================================================

if __name__ == "__main__":
    try:
        auth_result = app.client.auth_test()
        BOT_USER_ID = auth_result["user_id"]
        logger.info(f"Yuri bot user ID: {BOT_USER_ID}")
    except Exception as e:
        logger.warning(f"Could not get bot user ID: {e}")

    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    logger.info("⚡️ Yuri bot is running!")
    handler.start()
