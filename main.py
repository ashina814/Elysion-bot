import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import aiosqlite
import datetime
import random
import uuid
import asyncio
import logging
import traceback
import math
import contextlib
import os
import glob
from typing import Optional, List, Dict
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler


# ── 環境変数とロギング ──
load_dotenv() 

# トークン取得
raw_token = os.getenv("DISCORD_TOKEN")
if raw_token:
    TOKEN = str(raw_token).strip().replace('"', '').replace("'", "")
else:
    TOKEN = None

# ロギング設定
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

if not TOKEN:
    logging.error("DISCORD_TOKEN is missing. Please check your Environment Variables or .env file.")
else:
    logging.info("DISCORD_TOKEN loaded successfully.")

# ログファイルの設定
file_handler = RotatingFileHandler(
    'stella_bank.log',
    maxBytes=5*1024*1024,
    backupCount=3,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger = logging.getLogger('StellaBank')
logger.addHandler(file_handler)


# ================================================================
#   カラーパレット
# ================================================================
class Color:
    STELL     = 0xFFD700  # STELL・銀行系（ゴールド）
    CESTA     = 0x9B59B6  # セスタ系（パープル）
    GAMBLE    = 0xE74C3C  # ギャンブル系（レッド）
    DARK      = 0x2B2D31  # VC・ランク・縁系（ダーク）
    TICKET    = 0x5865F2  # チケット系（ブルー）
    SYSTEM    = 0x57595D  # 管理・システム系（グレー）
    SUCCESS   = 0x57F287  # 成功・完了系（グリーン）
    DANGER    = 0xFF4444  # 警告・エラー系（レッド）
    STOCK     = 0x1ABC9C  # 株・市場系（ティール）

# ── 設定管理・権限チェックシステム ──

class ConfigManager:
    def __init__(self, bot):
        self.bot = bot
        self.vc_reward_per_min: int = 10
        self.role_wages: Dict[int, int] = {}       
        self.admin_roles: Dict[int, str] = {}      

    async def reload(self):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'vc_reward'") as cursor:
                row = await cursor.fetchone()
                if row: self.vc_reward_per_min = int(row['value'])
            
            async with db.execute("SELECT role_id, amount FROM role_wages") as cursor:
                rows = await cursor.fetchall()
                self.role_wages = {r['role_id']: r['amount'] for r in rows}

            async with db.execute("SELECT role_id, perm_level FROM admin_roles") as cursor:
                rows = await cursor.fetchall()
                self.admin_roles = {r['role_id']: r['perm_level'] for r in rows}
        logger.info("Configuration and Permissions reloaded.")

def has_permission(required_level: str):
    async def predicate(interaction: discord.Interaction) -> bool:
        if await interaction.client.is_owner(interaction.user):
            return True
        
        user_role_ids = [role.id for role in interaction.user.roles]
        admin_roles = interaction.client.config.admin_roles
        
        # 権限レベルの強さ定義
        levels = ["SUPREME_GOD", "GODDESS", "ADMIN"]
        try:
            req_index = levels.index(required_level)
        except ValueError:
            req_index = len(levels) # 未知のレベル

        for r_id in user_role_ids:
            if r_id in admin_roles:
                user_level = admin_roles[r_id]
                try:
                    user_index = levels.index(user_level)
                    if user_index <= req_index: # インデックスが小さいほど偉い
                        return True
                except ValueError:
                    continue
        
        raise app_commands.AppCommandError(f"この操作には '{required_level}' 以上の権限が必要です。")
    return app_commands.check(predicate)

class BankDatabase:
    def __init__(self, db_path="stella_bank_v1.db"):
        self.db_path = db_path

    async def setup(self, conn):
        # 高速化設定
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys = ON") 

        # 1. 口座・取引
        await conn.execute("""CREATE TABLE IF NOT EXISTS accounts (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 0 CHECK(balance >= 0), 
            total_earned INTEGER DEFAULT 0
        )""")
        await conn.execute("INSERT OR IGNORE INTO accounts (user_id, balance, total_earned) VALUES (0, 0, 0)")
        # ▲▲▲ ここまで ▲▲▲

        await conn.execute("""CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER REFERENCES accounts(user_id),
            receiver_id INTEGER REFERENCES accounts(user_id),
            amount INTEGER,
            type TEXT,
            batch_id TEXT,
            month_tag TEXT,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        # 2. 設定・権限
        await conn.execute("CREATE TABLE IF NOT EXISTS server_config (key TEXT PRIMARY KEY, value TEXT)")
        await conn.execute("CREATE TABLE IF NOT EXISTS role_wages (role_id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)")
        await conn.execute("CREATE TABLE IF NOT EXISTS admin_roles (role_id INTEGER PRIMARY KEY, perm_level TEXT)")
        
        # ユーザーごとの設定
        await conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY, 
            dm_salary_enabled INTEGER DEFAULT 1
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS voice_stats (
            user_id INTEGER, 
            month TEXT, 
            total_seconds INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, month)
        )""")
        
        await conn.execute("CREATE TABLE IF NOT EXISTS voice_tracking (user_id INTEGER PRIMARY KEY, join_time TEXT)")
        await conn.execute("""CREATE TABLE IF NOT EXISTS temp_vcs (
            channel_id INTEGER PRIMARY KEY,
            guild_id INTEGER,
            owner_id INTEGER,
            expire_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        await conn.execute("CREATE TABLE IF NOT EXISTS reward_channels (channel_id INTEGER PRIMARY KEY)")

        # VC在室時間ランキング用（全VC対象）
        await conn.execute("""CREATE TABLE IF NOT EXISTS vc_rank_stats (
            user_id INTEGER,
            month TEXT,
            total_seconds INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, month)
        )""")

        # メッセージ数ランキング用
        await conn.execute("""CREATE TABLE IF NOT EXISTS message_stats (
            user_id INTEGER,
            month TEXT,
            count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, month)
        )""")

        # レベルシステム用（累計）
        await conn.execute("""CREATE TABLE IF NOT EXISTS user_levels (
            user_id INTEGER PRIMARY KEY,
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 0,
            total_vc_seconds INTEGER DEFAULT 0,
            total_messages INTEGER DEFAULT 0
        )""")

        # 縁システム用
        await conn.execute("""CREATE TABLE IF NOT EXISTS bonds (
            user_a INTEGER,
            user_b INTEGER,
            total_seconds INTEGER DEFAULT 0,
            rank TEXT DEFAULT '',
            PRIMARY KEY (user_a, user_b)
        )""")

        # 4. インデックス
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trans_receiver ON transactions (receiver_id, created_at DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_temp_vc_expire ON temp_vcs (expire_at)")
        

        # 5. ショップ・スロット・統計
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS shop_items (
                role_id TEXT,
                shop_id TEXT,
                price INTEGER,
                description TEXT,
                item_type TEXT DEFAULT 'rental',
                max_per_user INTEGER DEFAULT 0,
                PRIMARY KEY (role_id, shop_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS shop_subscriptions (
                user_id INTEGER,
                role_id INTEGER,
                expiry_date TEXT,
                PRIMARY KEY (user_id, role_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ticket_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                shop_id TEXT,
                item_key TEXT,
                item_name TEXT,
                purchased_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                used_at DATETIME,
                used_by INTEGER
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lottery_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                number INTEGER
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS slot_states (
                user_id INTEGER PRIMARY KEY,
                spins_since_win INTEGER DEFAULT 0
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date          TEXT PRIMARY KEY,
                total_stell   INTEGER DEFAULT 0,
                total_cesta   INTEGER DEFAULT 0,
                gini          REAL    DEFAULT 0
            )
        """)
                
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_issuers (
                user_id INTEGER PRIMARY KEY,
                total_shares INTEGER DEFAULT 0,
                is_listed INTEGER DEFAULT 1
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_holdings (
                user_id INTEGER,
                issuer_id INTEGER,
                amount INTEGER,
                avg_cost REAL,
                PRIMARY KEY (user_id, issuer_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS market_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_play_counts (
                user_id INTEGER,
                game TEXT,
                date TEXT,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, game, date)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_play_exemptions (
                user_id INTEGER,
                game TEXT,
                date TEXT,
                PRIMARY KEY (user_id, game, date)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cesta_wallets (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0 CHECK(balance >= 0)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cesta_daily_claims (
                user_id INTEGER PRIMARY KEY,
                last_claim TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cesta_daily_purchases (
                user_id INTEGER,
                date TEXT,
                amount INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, date)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS slot_cooldowns (
                user_id INTEGER PRIMARY KEY,
                last_play TEXT,
                bigwin_until TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS slot_streaks (
                user_id INTEGER PRIMARY KEY,
                win_streak INTEGER DEFAULT 0,
                lose_streak INTEGER DEFAULT 0
            )
        """)
# セスタショップ関連
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_badges (
            user_id    INTEGER,
            badge_id   TEXT,
            granted_at TEXT,
            PRIMARY KEY (user_id, badge_id)
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_spent (
            user_id       INTEGER PRIMARY KEY,
            total_spent   INTEGER DEFAULT 0
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_shop_items (
            item_id      TEXT PRIMARY KEY,
            name         TEXT,
            description  TEXT,
            price        INTEGER,
            item_type    TEXT,
            required_badge TEXT,
            role_id      INTEGER,
            duration_days INTEGER DEFAULT 0
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_shop_subs (
            user_id    INTEGER,
            item_id    TEXT,
            expiry     TEXT,
            PRIMARY KEY (user_id, item_id)
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_tickets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER,
            item_id      TEXT,
            item_name    TEXT,
            purchased_at TEXT,
            used_at      TEXT,
            used_by      INTEGER
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS cesta_badge_thresholds (
            badge_id     TEXT PRIMARY KEY,
            threshold    INTEGER
        )""")
        # デフォルト閾値を挿入
        await conn.execute("INSERT OR IGNORE INTO cesta_badge_thresholds VALUES ('入場券', 100)")
        await conn.execute("INSERT OR IGNORE INTO cesta_badge_thresholds VALUES ('道化師の証', 500)")
        await conn.execute("INSERT OR IGNORE INTO cesta_badge_thresholds VALUES ('座長の印', 2000)")

        await conn.execute("""CREATE TABLE IF NOT EXISTS ticket_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS ticket_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            emoji TEXT,
            description TEXT
        )""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER UNIQUE,
            user_id INTEGER,
            type_name TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at DATETIME,
            closed_by INTEGER
        )""")
        
        await conn.commit()


# ── UI: VC内操作パネル ──
class VCControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="招待するメンバーを選択...", min_values=1, max_values=10, row=0, custom_id="vc_invite_select")
    async def invite_users(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        await interaction.response.defer(ephemeral=True)
        
        channel = interaction.channel
        if not isinstance(channel, discord.VoiceChannel):
            return await interaction.followup.send("❌ ここはボイスチャンネルではありません。", ephemeral=True)

        perms = discord.PermissionOverwrite(
            view_channel=True, connect=True, speak=True, stream=True,
            use_voice_activation=True, send_messages=True, read_message_history=True
        )

        added_users = []
        for member in select.values:
            if member.bot: continue
            await channel.set_permissions(member, overwrite=perms)
            added_users.append(member.display_name)

        if not added_users:
            return await interaction.followup.send("❌ 招待できるメンバーがいませんでした。", ephemeral=True)

        await interaction.followup.send(f"✅ 以下のメンバーを招待しました:\n{', '.join(added_users)}", ephemeral=True)
        await channel.send(f"👋 {interaction.user.mention} が {', '.join([m.mention for m in select.values if not m.bot])} を招待しました。")

    @discord.ui.button(label="メンバーの権限を剥奪(追放)", style=discord.ButtonStyle.danger, row=1, custom_id="vc_kick_btn")
    async def kick_user_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RemoveUserView()
        await interaction.response.send_message("権限を剥奪するメンバーを選んでください。", view=view, ephemeral=True)


class RemoveUserView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="権限を剥奪するメンバーを選択...", min_values=1, max_values=10, custom_id="vc_remove_select")
    async def remove_users(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        await interaction.response.defer(ephemeral=True)
        channel = interaction.channel

        removed_names = []
        for member in select.values:
            if member.id == interaction.user.id: continue
            if member.bot: continue
            await channel.set_permissions(member, overwrite=None)
            if member.voice and member.voice.channel and member.voice.channel.id == channel.id:
                await member.move_to(None)
            removed_names.append(member.display_name)

        if removed_names:
            await interaction.followup.send(f"🚫 以下のメンバーの権限を剥奪しました:\n{', '.join(removed_names)}", ephemeral=True)
        else:
            await interaction.followup.send("❌ 対象を選択してください（自分自身は削除できません）。", ephemeral=True)


# ── UI: プラン選択メニュー ──
class PlanSelect(discord.ui.Select):
    def __init__(self, prices: dict):
        self.prices = prices
        options = [
            discord.SelectOption(label="6時間プラン",  description=f"{prices.get('6',  5000):,} Stell - ちょっとした作業や会議に", value="6",  emoji="🕐"),
            discord.SelectOption(label="12時間プラン", description=f"{prices.get('12', 10000):,} Stell - 半日じっくり",             value="12", emoji="🕓"),
            discord.SelectOption(label="24時間プラン", description=f"{prices.get('24', 30000):,} Stell - 丸一日貸切",               value="24", emoji="🕛"),
        ]
        super().__init__(placeholder="利用プランを選択してください...", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        user = interaction.user
        bot = interaction.client
        async with bot.get_db() as db:
            async with db.execute("SELECT channel_id FROM temp_vcs WHERE owner_id = ?", (user.id,)) as cursor:
                existing = await cursor.fetchone()

            if existing:
                # チャンネルが実際に存在するか確認
                real_channel = bot.get_channel(existing['channel_id'])
                if real_channel is None:
                    # 実在しない → 孤立レコードなので削除してOK
                    await db.execute("DELETE FROM temp_vcs WHERE owner_id = ?", (user.id,))
                    await db.commit()
                else:
                    return await interaction.followup.send("❌ あなたは既に一時VCを作成しています。", ephemeral=True)

        hours = int(self.values[0])
        price = self.prices.get(str(hours), 5000)

        async with bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (user.id,)) as cursor:
                row = await cursor.fetchone()
                current_bal = row['balance'] if row else 0

            if current_bal < price:
                return await interaction.followup.send(
                    f"❌ 残高不足です。\n必要: {price:,} Stell / 所持: {current_bal:,} Stell", ephemeral=True
                )

            month_tag = datetime.datetime.now().strftime("%Y-%m")
            await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (price, user.id))
            await db.execute(
                "INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag) VALUES (?, 0, ?, 'VC_CREATE', ?, ?)",
                (user.id, price, f"一時VC作成 ({hours}時間)", month_tag)
            )
            await db.commit()

        try:
            guild = interaction.guild
            category = interaction.channel.category

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False),
                user: discord.PermissionOverwrite(
                    view_channel=True, connect=True, speak=True, stream=True,
                    use_voice_activation=True, send_messages=True, read_message_history=True,
                    move_members=True, mute_members=True
                ),
                guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True)
            }

            channel_name = f"🔒 {user.display_name}の部屋"
            if not category:
                new_vc = await guild.create_voice_channel(name=channel_name, overwrites=overwrites, user_limit=2)
            else:
                new_vc = await guild.create_voice_channel(name=channel_name, category=category, overwrites=overwrites, user_limit=2)

            expire_dt = datetime.datetime.now() + datetime.timedelta(hours=hours)
            async with bot.get_db() as db:
                await db.execute(
                    "INSERT INTO temp_vcs (channel_id, guild_id, owner_id, expire_at) VALUES (?, ?, ?, ?)",
                    (new_vc.id, guild.id, user.id, expire_dt)
                )
                await db.commit()

            await new_vc.send(
                f"{user.mention} ようこそ！\nこのパネルを使って、友達を招待したり権限を管理できます。\n(時間が来るとこのチャンネルは自動消滅します)",
                view=VCControlView()
            )
            await interaction.followup.send(
                f"✅ 作成完了: {new_vc.mention}\n期限: {expire_dt.strftime('%m/%d %H:%M')}\n招待機能はチャンネル内のパネルを使用してください。",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"VC Create Error: {e}")
            async with bot.get_db() as db:
                await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (price, user.id))
                await db.commit()
            await interaction.followup.send("❌ VC作成中にエラーが発生しました。料金を返金しました。", ephemeral=True)

class PublicPlanSelect(discord.ui.Select):
    def __init__(self, prices: dict):
        self.prices = prices
        options = [
            discord.SelectOption(label="6時間プラン",  description=f"{prices.get('6',  10000):,} Stell", value="6",  emoji="🕐"),
            discord.SelectOption(label="12時間プラン", description=f"{prices.get('12', 30000):,} Stell", value="12", emoji="🕓"),
            discord.SelectOption(label="24時間プラン", description=f"{prices.get('24', 50000):,} Stell", value="24", emoji="🕛"),
        ]
        super().__init__(placeholder="利用プランを選択してください...", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        user = interaction.user
        bot  = interaction.client
        hours = int(self.values[0])
        price = self.prices.get(str(hours), 10000)

        # 既存VCチェック（プライベート版と共通）
        async with bot.get_db() as db:
            async with db.execute("SELECT channel_id FROM temp_vcs WHERE owner_id = ?", (user.id,)) as cursor:
                existing = await cursor.fetchone()
            if existing:
                real_channel = bot.get_channel(existing['channel_id'])
                if real_channel is None:
                    await db.execute("DELETE FROM temp_vcs WHERE owner_id = ?", (user.id,))
                    await db.commit()
                else:
                    return await interaction.followup.send("❌ あなたは既に一時VCを作成しています。", ephemeral=True)

        # 残高チェック
        async with bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (user.id,)) as cursor:
                row = await cursor.fetchone()
                current_bal = row['balance'] if row else 0
            if current_bal < price:
                return await interaction.followup.send(
                    f"❌ 残高不足です。\n必要: {price:,} Stell / 所持: {current_bal:,} Stell", ephemeral=True
                )

            # 除外ロールを取得
            async with db.execute("SELECT value FROM server_config WHERE key = 'public_vc_exclude_roles'") as c:
                row = await c.fetchone()
            exclude_ids = [int(x) for x in row['value'].split(',') if x] if row and row['value'] else []

            month_tag = datetime.datetime.now().strftime("%Y-%m")
            await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (price, user.id))
            await db.execute(
                "INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag) VALUES (?, 0, ?, 'PUBLIC_VC_CREATE', ?, ?)",
                (user.id, price, f"公開VC作成 ({hours}時間)", month_tag)
            )
            await db.commit()

        try:
            guild    = interaction.guild
            category = interaction.channel.category

            # 除外ロールは拒否、それ以外は全員OK
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True),
                user: discord.PermissionOverwrite(
                    view_channel=True, connect=True, speak=True, stream=True,
                    use_voice_activation=True, send_messages=True, read_message_history=True,
                    move_members=True, mute_members=True
                ),
            }
            for role_id in exclude_ids:
                role = guild.get_role(role_id)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(view_channel=False, connect=False)

            channel_name = f"🔓 {user.display_name}の部屋"
            if not category:
                new_vc = await guild.create_voice_channel(name=channel_name, overwrites=overwrites)
            else:
                new_vc = await guild.create_voice_channel(name=channel_name, category=category, overwrites=overwrites)

            expire_dt = datetime.datetime.now() + datetime.timedelta(hours=hours)
            async with bot.get_db() as db:
                await db.execute(
                    "INSERT INTO temp_vcs (channel_id, guild_id, owner_id, expire_at) VALUES (?, ?, ?, ?)",
                    (new_vc.id, guild.id, user.id, expire_dt)
                )
                await db.commit()

            await interaction.followup.send(
                f"✅ 公開VC作成完了: {new_vc.mention}\n期限: {expire_dt.strftime('%m/%d %H:%M')}",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Public VC Create Error: {e}")
            async with bot.get_db() as db:
                await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (price, user.id))
                await db.commit()
            await interaction.followup.send("❌ VC作成中にエラーが発生しました。料金を返金しました。", ephemeral=True)


class PublicVCPanel(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="公開VCを作成する", style=discord.ButtonStyle.primary, custom_id="create_public_vc_btn", emoji="🔓")
    async def create_vc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = interaction.client
        prices = {}
        async with bot.get_db() as db:
            async with db.execute("SELECT key, value FROM server_config WHERE key IN ('public_vc_price_6', 'public_vc_price_12', 'public_vc_price_24')") as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    prices[row['key'].replace('public_vc_price_', '')] = int(row['value'])

        if '6'  not in prices: prices['6']  = 10000
        if '12' not in prices: prices['12'] = 30000
        if '24' not in prices: prices['24'] = 50000

        view = discord.ui.View()
        view.add_item(PublicPlanSelect(prices))
        await interaction.response.send_message("利用する時間プランを選択してください。", view=view, ephemeral=True)
        
class VCPanel(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="一時VCを作成する", style=discord.ButtonStyle.success, custom_id="create_temp_vc_btn", emoji="🔒")
    async def create_vc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = interaction.client
        prices = {}
        async with bot.get_db() as db:
            async with db.execute("SELECT key, value FROM server_config WHERE key IN ('vc_price_6', 'vc_price_12', 'vc_price_24')") as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    prices[row['key'].replace('vc_price_', '')] = int(row['value'])

        if '6'  not in prices: prices['6']  = 30000
        if '12' not in prices: prices['12'] = 50000
        if '24' not in prices: prices['24'] = 80000

        view = discord.ui.View()
        view.add_item(PlanSelect(prices))
        await interaction.response.send_message("利用する時間プランを選択してください。", view=view, ephemeral=True)


# ── Cog: PrivateVCManager ──
class PrivateVCManager(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_expiration_task.start()

    def cog_unload(self):
        self.check_expiration_task.cancel()

    @tasks.loop(minutes=1)
    async def check_expiration_task(self):
        now = datetime.datetime.now()
        try:
            async with self.bot.get_db() as db:
                async with db.execute("SELECT channel_id, guild_id FROM temp_vcs") as cursor:
                    all_vcs = await cursor.fetchall()

                if not all_vcs: return

                for row in all_vcs:
                    c_id = row['channel_id']
                    channel = self.bot.get_channel(c_id)
                    if channel is None:
                        await db.execute("DELETE FROM temp_vcs WHERE channel_id = ?", (c_id,))
                    else:
                        async with db.execute("SELECT expire_at FROM temp_vcs WHERE channel_id = ?", (c_id,)) as c:
                            rec = await c.fetchone()
                        if rec:
                            expire_at = datetime.datetime.fromisoformat(str(rec['expire_at']))
                            if now >= expire_at:
                                try:
                                    await channel.delete(reason="Temp VC Expired")
                                except: pass
                                await db.execute("DELETE FROM temp_vcs WHERE channel_id = ?", (c_id,))

                await db.commit()
        except Exception as e:
            logger.error(f"Expiration Check Error: {e}")

    @check_expiration_task.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="一時vcパネル作成", description="内容をカスタマイズしてVC作成パネルを設置します")
    @app_commands.describe(
        title="パネルのタイトル",
        description="パネルの説明文（\\nで改行）",
        price_6h="6時間プランの価格",
        price_12h="12時間プランの価格",
        price_24h="24時間プランの価格"
    )
    @has_permission("ADMIN")
    async def deploy_panel(
        self,
        interaction: discord.Interaction,
        title: str = "アパホテル",
        description: str = None,
        price_6h: int = 5000,
        price_12h: int = 10000,
        price_24h: int = 30000
    ):
        await interaction.response.defer(ephemeral=True)

        if description is None:
            description = (
                "権限のある人以外からは見えない、プライベートな一時VCを作成できます。ようこそアパホテルへ\n\n"
                "**🔒 プライバシー**\n招待した人以外は見えません\n"
                "**🛡 料金システム**\n作成時に自動引き落とし\n"
                f"**⏰ 料金プラン**\n"
                f"• **6時間**: {price_6h:,} Stell\n"
                f"• **12時間**: {price_12h:,} Stell\n"
                f"• **24時間**: {price_24h:,} Stell"
            )
        else:
            description = description.replace("\\n", "\n")

        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('vc_price_6', ?)",  (str(price_6h),))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('vc_price_12', ?)", (str(price_12h),))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('vc_price_24', ?)", (str(price_24h),))
            await db.commit()

        embed = discord.Embed(title=title, description=description, color=Color.DARK)
        embed.set_footer(text=f"Last Updated: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M')}")

        await interaction.channel.send(embed=embed, view=VCPanel())
        await interaction.followup.send("✅ 設定を保存し、パネルを設置しました。", ephemeral=True)

# ── 公開VC用: 除外ロール設定 ──
    @app_commands.command(name="公開vc除外ロール設定", description="【管理者】公開VCに入れないロールを設定します")
    @app_commands.describe(action="追加か削除か", role="対象ロール")
    @app_commands.choices(action=[
        app_commands.Choice(name="追加", value="add"),
        app_commands.Choice(name="削除", value="remove"),
        app_commands.Choice(name="一覧確認", value="list"),
    ])
    @has_permission("ADMIN")
    async def config_public_vc_exclude(self, interaction: discord.Interaction, action: str, role: Optional[discord.Role] = None):
        await interaction.response.defer(ephemeral=True)

        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'public_vc_exclude_roles'") as c:
                row = await c.fetchone()
            current = row['value'].split(',') if row and row['value'] else []

        if action == "list":
            if not current:
                return await interaction.followup.send("除外ロールは設定されていません。", ephemeral=True)
            mentions = "\n".join(f"<@&{r}>" for r in current if r)
            embed = discord.Embed(title="🚫 公開VC除外ロール一覧", description=mentions, color=Color.DANGER)
            return await interaction.followup.send(embed=embed, ephemeral=True)

        if not role:
            return await interaction.followup.send("❌ ロールを指定してください。", ephemeral=True)

        if action == "add":
            if str(role.id) in current:
                return await interaction.followup.send(f"⚠️ {role.mention} は既に登録されています。", ephemeral=True)
            current.append(str(role.id))
            msg = f"✅ {role.mention} を除外ロールに追加しました。"
        else:
            if str(role.id) not in current:
                return await interaction.followup.send(f"⚠️ {role.mention} は登録されていません。", ephemeral=True)
            current.remove(str(role.id))
            msg = f"🗑️ {role.mention} を除外ロールから削除しました。"

        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('public_vc_exclude_roles', ?)", (','.join(current),))
            await db.commit()

        await interaction.followup.send(msg, ephemeral=True)

    # ── 公開VC用: パネル設置 ──
    @app_commands.command(name="公開vcパネル作成", description="公開一時VCの作成パネルを設置します")
    @app_commands.describe(
        title="パネルのタイトル",
        description="パネルの説明文（\\nで改行）",
        price_6h="6時間プランの価格",
        price_12h="12時間プランの価格",
        price_24h="24時間プランの価格"
    )
    @has_permission("ADMIN")
    async def deploy_public_panel(
        self,
        interaction: discord.Interaction,
        title: str = "公開ルーム",
        description: str = None,
        price_6h: int = 10000,
        price_12h: int = 30000,
        price_24h: int = 50000
    ):
        await interaction.response.defer(ephemeral=True)

        if description is None:
            description = (
                "誰でも入れる公開一時VCを作成できます。\n\n"
                "**🔓 公開ルーム**\n設定された一部のロールを除き誰でも参加できます\n"
                "**🛡 料金システム**\n作成時に自動引き落とし\n"
                f"**⏰ 料金プラン**\n"
                f"• **6時間**: {price_6h:,} Stell\n"
                f"• **12時間**: {price_12h:,} Stell\n"
                f"• **24時間**: {price_24h:,} Stell"
            )
        else:
            description = description.replace("\\n", "\n")

        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('public_vc_price_6', ?)",  (str(price_6h),))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('public_vc_price_12', ?)", (str(price_12h),))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('public_vc_price_24', ?)", (str(price_24h),))
            await db.commit()

        embed = discord.Embed(title=title, description=description, color=Color.DARK)
        embed.set_footer(text=f"Last Updated: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M')}")

        await interaction.channel.send(embed=embed, view=PublicVCPanel())
        await interaction.followup.send("✅ 設定を保存し、公開VCパネルを設置しました。", ephemeral=True)


class TransferConfirmView(discord.ui.View):
    def __init__(self, bot, sender, receiver, amount, message):
        super().__init__(timeout=60)
        self.bot = bot
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.msg = message
        self.processed = False

    async def on_timeout(self):
        if not self.processed:
            for child in self.children:
                child.disabled = True
            try:
                await self.message.edit(content="⏰ 時間切れです。", view=self)
            except:
                pass

    @discord.ui.button(label="✅ 送金を実行する", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.processed: return
        self.processed = True
        
        if interaction.user.id != self.sender.id:
            return await interaction.response.send_message("❌ 操作権限がありません。", ephemeral=True)

        await interaction.response.defer()
        
        month_tag = datetime.datetime.now().strftime("%Y-%m")
        sender_new_bal = 0
        receiver_new_bal = 0

        async with self.bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (self.sender.id,)) as c:
                row = await c.fetchone()
                if not row or row['balance'] < self.amount:
                    return await interaction.followup.send("❌ 残高が不足しています。", ephemeral=True)

            try:
                await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (self.amount, self.sender.id))
                
                await db.execute("""
                    INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, ?, 0)
                    ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance
                """, (self.receiver.id, self.amount))
                
                await db.execute("""
                    INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                    VALUES (?, ?, ?, 'TRANSFER', ?, ?)
                """, (self.sender.id, self.receiver.id, self.amount, self.msg, month_tag))
                
                async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (self.sender.id,)) as c:
                    sender_new_bal = (await c.fetchone())['balance']
                async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (self.receiver.id,)) as c:
                    receiver_new_bal = (await c.fetchone())['balance']

                await db.commit()
                
                self.stop()
                await interaction.edit_original_response(content=f"✅ {self.receiver.mention} へ {self.amount:,} Stell 送金しました。", embed=None, view=None)

                try:
                    notify = True
                    async with db.execute("SELECT dm_salary_enabled FROM user_settings WHERE user_id = ?", (self.receiver.id,)) as c:
                        res = await c.fetchone()
                        if res and res['dm_salary_enabled'] == 0: notify = False
                    
                    if notify:
                        embed = discord.Embed(title="💰 Stell受取通知", color=Color.SUCCESS)
                        embed.add_field(name="送金者", value=self.sender.mention, inline=False)
                        embed.add_field(name="受取額", value=f"**{self.amount:,} Stell**", inline=False)
                        embed.add_field(name="メッセージ", value=f"`{self.msg}`", inline=False)
                        embed.timestamp = datetime.datetime.now()
                        await self.receiver.send(embed=embed)
                except:
                    pass

                log_ch_id = None
                async with db.execute("SELECT value FROM server_config WHERE key = 'currency_log_id'") as c:
                    row = await c.fetchone()
                    if row: log_ch_id = int(row['value'])
                
                if log_ch_id:
                    channel = self.bot.get_channel(log_ch_id)
                    if channel:
                        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        log_embed = discord.Embed(title="💸 送金ログ", color=Color.STELL)
                        log_embed.description = f"{self.sender.mention} ➔ {self.receiver.mention}"
                        log_embed.add_field(name="金額", value=f"**{self.amount:,} Stell**", inline=True)
                        log_embed.add_field(name="備考", value=self.msg, inline=True)
                        log_embed.add_field(name="処理後残高", value=f"送: {sender_new_bal:,} Stell\n受: {receiver_new_bal:,} Stell", inline=False)
                        log_embed.set_footer(text=f"Time: {now_str}")
                        await channel.send(embed=log_embed)

            except Exception as e:
                await db.rollback()
                await interaction.followup.send(f"❌ エラーが発生しました: {e}", ephemeral=True)

    @discord.ui.button(label="❌ キャンセル", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.processed: return
        self.processed = True
        
        if interaction.user.id != self.sender.id:
            return await interaction.response.send_message("❌ 操作権限がありません。", ephemeral=True)

        self.stop()
        await interaction.response.edit_message(content="❌ 送金をキャンセルしました。", embed=None, view=None)

# ── Cog: Economy (残高・送金・ランキング・資金操作) ──
class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="ping", description="Botの応答速度を確認します")
    @has_permission("ADMIN")
    async def ping(self, interaction: discord.Interaction):
        latency = round(self.bot.latency * 1000)
        await interaction.response.send_message(f"🏓 Pong! Latency: `{latency}ms`", ephemeral=True)

    @app_commands.command(name="残高確認", description="現在の所持金を確認します")
    async def balance(self, interaction: discord.Interaction, member: Optional[discord.Member] = None):
        await interaction.response.defer(ephemeral=True)
        target = member or interaction.user
        
        if target.id != interaction.user.id:
            if not await self.check_admin_permission(interaction.user):
                return await interaction.followup.send("❌ 他人の口座を参照する権限がありません。", ephemeral=True)

        async with self.bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (target.id,)) as cursor:
                row = await cursor.fetchone()
                bal = row['balance'] if row else 0
        
        embed = discord.Embed(title="💰 口座残高", color=Color.STELL)
        embed.set_author(name=f"{target.display_name} 様", icon_url=target.display_avatar.url)
        embed.add_field(name="💰 現在の残高", value=f"**{bal:,} Stell**", inline=False)
        embed.set_footer(text="Stella Bank")
        embed.set_thumbnail(url=target.display_avatar.url)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="送金", description="他のユーザーにStellを送金します")
    @app_commands.describe(receiver="送金相手", amount="送金額", message="相手へのメッセージ（任意）")
    async def transfer(self, interaction: discord.Interaction, receiver: discord.Member, amount: int, message: str = "送金"):
        if amount <= 0: return await interaction.response.send_message("❌ 1 Stell 以上を指定してください。", ephemeral=True)
        if amount > 10000000: return await interaction.response.send_message("❌ 1回の送金上限は 10,000,000 Stell です。", ephemeral=True)
        if receiver.id == interaction.user.id: return await interaction.response.send_message("❌ 自分自身には送金できません。", ephemeral=True)
        if receiver.bot: return await interaction.response.send_message("❌ Botには送金できません。", ephemeral=True)

        embed = discord.Embed(title="⚠️ 送金確認", description="以下の内容で送金しますか？", color=Color.STELL)
        embed.add_field(name="👤 送金先", value=receiver.mention, inline=True)
        embed.add_field(name="💰 金額", value=f"**{amount:,} Stell**", inline=True)
        embed.add_field(name="💬 メッセージ", value=f"`{message}`", inline=False)
        
        view = TransferConfirmView(self.bot, interaction.user, receiver, amount, message)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="履歴", description="直近10件の入出金履歴を表示します")
    async def history(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            query = "SELECT * FROM transactions WHERE sender_id = ? OR receiver_id = ? ORDER BY created_at DESC LIMIT 10"
            async with db.execute(query, (interaction.user.id, interaction.user.id)) as cursor:
                rows = await cursor.fetchall()
        
        if not rows: return await interaction.followup.send("取引履歴はありません。", ephemeral=True)

        embed = discord.Embed(title="📜 取引履歴明細", color=Color.TICKET)
        for r in rows:
            is_sender = r['sender_id'] == interaction.user.id
            emoji = "📤 送金" if is_sender else "📥 受取"
            amount_str = f"{'-' if is_sender else '+'}{r['amount']:,} Stell"
            
            target_id = r['receiver_id'] if is_sender else r['sender_id']
            target_name = f"<@{target_id}>" if target_id != 0 else "システム"

            embed.add_field(
                name=f"{r['created_at'][5:16]} | {emoji}",
                value=f"金額: **{amount_str}**\n相手: {target_name}\n内容: `{r['description']}`",
                inline=False
            )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="今日の残り回数", description="今日のギャンブル残り回数を確認します")
    async def check_remaining(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        today   = datetime.datetime.now().strftime("%Y-%m-%d")

        bj_limit        = await _cfg(self.bot, "slot_daily_limit")
        chinchiro_limit = await _cfg(self.bot, "chinchiro_daily_limit")

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT count FROM daily_play_counts WHERE user_id=? AND game='blackjack' AND date=?",
                (user_id, today)
            ) as c:
                row = await c.fetchone()
            bj_count = row["count"] if row else 0

            async with db.execute(
                "SELECT 1 FROM daily_play_exemptions WHERE user_id=? AND game='blackjack' AND date=?",
                (user_id, today)
            ) as c:
                bj_exempt = bool(await c.fetchone())

            async with db.execute(
                "SELECT count FROM daily_play_counts WHERE user_id=? AND game='chinchiro' AND date=?",
                (user_id, today)
            ) as c:
                row = await c.fetchone()
            chinchiro_count = row["count"] if row else 0

            async with db.execute(
                "SELECT 1 FROM daily_play_exemptions WHERE user_id=? AND game='chinchiro' AND date=?",
                (user_id, today)
            ) as c:
                chinchiro_exempt = bool(await c.fetchone())

        embed = discord.Embed(title="🎲 本日のギャンブル残り回数", color=Color.DARK)
        embed.add_field(
            name="🎲 チンチロ",
            value="✨ 制限解除中" if chinchiro_exempt else f"残り **{max(chinchiro_limit - chinchiro_count, 0)} / {chinchiro_limit}** 回",
            inline=True
        )
        embed.add_field(
            name="🃏 ブラックジャック",
            value="✨ 制限解除中" if bj_exempt else f"残り **{max(bj_limit - bj_count, 0)} / {bj_limit}** 回",
            inline=True
        )
        embed.set_footer(text="制限は毎日0時にリセットされます")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    # === ゴミ拾い ===
    @app_commands.command(name="ゴミ拾い", description="ゴミを拾ってStellを稼ぎます（残高500以下限定・1日30回まで）")
    async def gomi_hiroi(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        today   = datetime.datetime.now().strftime("%Y-%m-%d")

        async with self.bot.get_db() as db:
            # 残高チェック
            async with db.execute(
                "SELECT balance FROM accounts WHERE user_id = ?", (user_id,)
            ) as c:
                row = await c.fetchone()
            bal = row["balance"] if row else 0

            if bal > 500:
                return await interaction.response.send_message(
                    "❌ 残高が500 Stellを超えているのでゴミ拾いはできません。",
                    ephemeral=True
                )

            # 日次上限チェック
            async with db.execute(
                "SELECT count FROM daily_play_counts WHERE user_id=? AND game='gomi' AND date=?",
                (user_id, today)
            ) as c:
                row = await c.fetchone()
            count = row["count"] if row else 0

            if count >= 30:
                return await interaction.response.send_message(
                    "🚫 今日のゴミ拾いは上限（30回）に達しました。また明日ね。",
                    ephemeral=True
                )

            # イースターエッグ抽選
            roll = random.random() * 100
            if roll < 0.1:
                # 釈迦から特別（0.1%）
                amount  = 10000
                gain    = amount
                message = "✨ 釈迦「**特別やで**」\n**10,000 Stell** もらった！"
            elif roll < 1.1:
                # 涅槃（1%）
                amount  = 0
                gain    = 0
                message = "🪷 涅槃に達した…お金への執着を手放した。\n**(+0 Stell)**"
            elif roll < 9.1:
                # 煩悩（8%）
                amount  = -random.randint(100, 300)
                gain    = max(amount, -bal)  # マイナスにならないよう調整
                message = f"😩 煩悩を拾ってしまった…108の苦しみ。\n**{gain:,} Stell**"
            elif roll < 14.1:
                # お賽銭（5%）
                amount  = random.randint(2000, 5000)
                gain    = amount
                message = f"👛 釈迦の財布を発見！功徳が積まれた！\n**+{gain:,} Stell**"
            elif roll < 29.1:
                # お賽銭（15%）
                amount  = random.randint(50, 200)
                gain    = amount
                message = f"🪙 お賽銭を拾った…ありがたや。\n**+{gain:,} Stell**"
            else:
                # 通常（76.9%）
                amount  = random.randint(500, 1000)
                gain    = amount
                message = f"🗑️ ゴミを拾って **+{gain:,} Stell** 稼いだ！"

            # 残高反映
            if gain != 0:
                await db.execute("""
                    INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, MAX(0, ?), MAX(0, ?))
                    ON CONFLICT(user_id) DO UPDATE SET
                        balance      = MAX(0, balance + ?),
                        total_earned = total_earned + MAX(0, ?)
                """, (user_id, gain, max(gain, 0), gain, max(gain, 0)))

                month_tag = datetime.datetime.now().strftime("%Y-%m")
                if gain > 0:
                    await db.execute("""
                        INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                        VALUES (0, ?, ?, 'GOMI', 'ゴミ拾い', ?)
                    """, (user_id, gain, month_tag))
                else:
                    await db.execute("""
                        INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                        VALUES (?, 0, ?, 'GOMI', 'ゴミ拾い（煩悩）', ?)
                    """, (user_id, abs(gain), month_tag))

            await db.execute("""
                INSERT INTO daily_play_counts (user_id, game, date, count) VALUES (?, 'gomi', ?, 1)
                ON CONFLICT(user_id, game, date) DO UPDATE SET count = count + 1
            """, (user_id, today))

            await db.commit()

        new_bal = max(0, bal + gain)
        remaining = 29 - count
        await interaction.response.send_message(
            f"{message}\n"
            f"残高: {new_bal:,} Stell　|　今日の残り: {remaining} 回",
            ephemeral=True
        )
        
    # === 追加機能1: 所持金ランキング ===
    @app_commands.command(name="ランキング", description="サーバー内の大富豪トップ10を表示します")
    async def ranking(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        async with self.bot.get_db() as db:
            # システムアカウント(ID:0)を除外し、残高が多い順に取得 (退出者やBotを飛ばせるように少し多めに取得)
            async with db.execute("SELECT user_id, balance FROM accounts WHERE user_id != 0 ORDER BY balance DESC LIMIT 30") as cursor:
                rows = await cursor.fetchall()

        if not rows:
            return await interaction.followup.send("まだデータがありません。")

        embed = discord.Embed(title="🏆 ステラ長者番付 トップ10", color=Color.STELL)
        embed.description = "サーバー内の大富豪ランキングです。\n\n"
        
        rank = 1
        for row in rows:
            if rank > 10: break
            
            member = interaction.guild.get_member(row['user_id'])
            # 退出済みのメンバーやBotはランキングから除外
            if not member or member.bot:
                continue
            
            medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"**{rank}.**"
            embed.description += f"{medal} **{member.display_name}**\n┗ 💰 **{row['balance']:,} Stell**\n\n"
            rank += 1

        embed.set_footer(text=f"実行者: {interaction.user.display_name} | Top 10 Richest Citizens")
        await interaction.followup.send(embed=embed)

# === 追加機能2: 資金の直接操作 ===
    @app_commands.command(name="資金操作", description="【最高神】指定したユーザーの所持金を直接増減させます")
    @app_commands.describe(
        target="操作対象のユーザー",
        action="増やすか、減らすか",
        amount="金額",
        reason="理由（ログに残ります）"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="➕ 増やす (Mint)", value="add"),
        app_commands.Choice(name="➖ 減らす (Burn)", value="remove")
    ])
    @has_permission("SUPREME_GOD")
    async def manipulate_funds(self, interaction: discord.Interaction, target: discord.Member, action: str, amount: int, reason: str = "システム操作"):
        if amount <= 0:
            return await interaction.response.send_message("❌ 1以上の金額を指定してください。", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        month_tag = datetime.datetime.now().strftime("%Y-%m")

        async with self.bot.get_db() as db:
            await db.execute("""
                INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, 0, 0)
                ON CONFLICT(user_id) DO NOTHING
            """, (target.id,))

            if action == "add":
                await db.execute("UPDATE accounts SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?", (amount, amount, target.id))
                await db.execute("""
                    INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                    VALUES (0, ?, ?, 'SYSTEM_ADD', ?, ?)
                """, (target.id, amount, f"【運営付与】{reason}", month_tag))
                msg = f"✅ {target.mention} に **{amount:,} Stell** を付与しました。\n理由: `{reason}`"
            
            else:
                async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (target.id,)) as c:
                    row = await c.fetchone()
                    current_bal = row['balance'] if row else 0
                
                actual_deduction = min(amount, current_bal)
                
                await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (actual_deduction, target.id))
                await db.execute("""
                    INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                    VALUES (?, 0, ?, 'SYSTEM_REMOVE', ?, ?)
                """, (target.id, actual_deduction, f"【運営没収】{reason}", month_tag))
                msg = f"✅ {target.mention} から **{actual_deduction:,} Stell** を没収しました。\n理由: `{reason}`"

            # ここを追加 ↓
            async with db.execute("SELECT value FROM server_config WHERE key = 'currency_log_id'") as c:
                row = await c.fetchone()
                log_ch_id = int(row['value']) if row else None

            await db.commit()

        embed = discord.Embed(title="⚙️ 運営資金操作ログ", color=Color.DANGER if action == "remove" else 0x00ff00)
        embed.add_field(name="対象", value=target.mention, inline=True)
        embed.add_field(name="操作", value="➕ 付与" if action == "add" else "➖ 没収", inline=True)
        embed.add_field(name="金額", value=f"**{amount:,} S**" if action == "add" else f"**{actual_deduction:,} S**", inline=True)
        embed.add_field(name="理由", value=reason, inline=False)
        embed.add_field(name="実行者", value=interaction.user.mention, inline=False)
        embed.timestamp = datetime.datetime.now()

        # ここを削除 ↓（元の2回目のget_dbブロックをこれに置き換え）
        if log_ch_id:
            channel = self.bot.get_channel(log_ch_id)
            if channel: await channel.send(embed=embed)

        await interaction.followup.send(msg, ephemeral=True)
            
        # 通貨ログチャンネルに通知を送る
        embed = discord.Embed(title="⚙️ 運営資金操作ログ", color=Color.DANGER if action == "remove" else 0x00ff00)
        embed.add_field(name="対象", value=target.mention, inline=True)
        embed.add_field(name="操作", value="➕ 付与" if action == "add" else "➖ 没収", inline=True)
        embed.add_field(name="金額", value=f"**{amount:,} S**" if action == "add" else f"**{actual_deduction:,} S**", inline=True)
        embed.add_field(name="理由", value=reason, inline=False)
        embed.add_field(name="実行者", value=interaction.user.mention, inline=False)
        embed.timestamp = datetime.datetime.now()

        log_ch_id = None
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'currency_log_id'") as c:
                row = await c.fetchone()
                if row: log_ch_id = int(row['value'])
        if log_ch_id:
            channel = self.bot.get_channel(log_ch_id)
            if channel: await channel.send(embed=embed)

        await interaction.followup.send(msg, ephemeral=True)

    async def check_admin_permission(self, user):
        if await self.bot.is_owner(user): return True
        user_role_ids = [role.id for role in user.roles]
        admin_roles = self.bot.config.admin_roles
        for r_id in user_role_ids:
            if r_id in admin_roles and admin_roles[r_id] in ["SUPREME_GOD", "GODDESS"]:
                return True
        return False


class Salary(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="通貨通知設定", description="通貨交換時のDM明細通知をON/OFFします")
    @app_commands.describe(status="ON: 通知を受け取る / OFF: 通知しない")
    @app_commands.choices(status=[
        app_commands.Choice(name="ON (通知する)", value=1),
        app_commands.Choice(name="OFF (通知しない)", value=0)
    ])
    async def toggle_dm(self, interaction: discord.Interaction, status: int):
        async with self.bot.get_db() as db:
            await db.execute("""
                INSERT INTO user_settings (user_id, dm_salary_enabled) 
                VALUES (?, ?) 
                ON CONFLICT(user_id) DO UPDATE SET dm_salary_enabled = excluded.dm_salary_enabled
            """, (interaction.user.id, status))
            await db.commit()
        
        msg = "✅ 今後、お金の明細は **DMで通知されます**。" if status == 1 else "🔕 今後、給与明細の **DM通知は行われません**。"
        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="一括給与", description="全役職の給与を合算支給し、明細をDM送信します")
    @has_permission("SUPREME_GOD")
    async def distribute_all(self, interaction: discord.Interaction):
        # 処理が長引く可能性があるため、タイムアウトを回避（最大15分猶予）
        await interaction.response.defer()
        
        now = datetime.datetime.now()
        month_tag = now.strftime("%Y-%m")
        batch_id = str(uuid.uuid4())[:8]
        
        # ── 1. データ準備 ──
        wage_dict = {}
        dm_prefs = {}
        async with self.bot.get_db() as db:
            async with db.execute("SELECT role_id, amount FROM role_wages") as c:
                async for r in c: wage_dict[int(r['role_id'])] = int(r['amount'])
            async with db.execute("SELECT user_id, dm_salary_enabled FROM user_settings") as c:
                async for r in c: dm_prefs[int(r['user_id'])] = bool(r['dm_salary_enabled'])

        if not wage_dict:
            return await interaction.followup.send("⚠️ 給与設定が見つかりません。")
        
        # メンバーリスト取得
        members = interaction.guild.members if interaction.guild.chunked else [m async for m in interaction.guild.fetch_members()]

        # ── 2. 計算処理（メモリ上で処理） ──
        count = 0
        total_payout = 0
        role_summary = {}
        payout_data_list = []

        # DB一括書き込み用のリスト
        account_updates = []
        transaction_inserts = []

        for member in members:
            if member.bot: continue
            
            matching = [(wage_dict[r.id], r) for r in member.roles if r.id in wage_dict]
            if not matching: continue
            
            member_total = sum(w for w, _ in matching)
            
            # DB書き込み用データをリストに追加 (SQLのパラメータ順に合わせる)
            # accounts: user_id, balance, total_earned
            account_updates.append((member.id, member_total, member_total))
            
            # transactions: sender, receiver, amount, type, batch_id, month, desc
            transaction_inserts.append((
                0, member.id, member_total, 'SALARY', batch_id, month_tag, f"{month_tag} 給与"
            ))

            count += 1
            total_payout += member_total
            
            # 集計用ロジック
            for w, r in matching:
                if r.id not in role_summary: role_summary[r.id] = {"mention": r.mention, "count": 0, "amount": 0}
                role_summary[r.id]["count"] += 1
                role_summary[r.id]["amount"] += w

            if dm_prefs.get(member.id, True):
                payout_data_list.append((member, member_total, matching))

        # ── 3. DB一括書き込み (高速化の肝) ──
        if account_updates:
            async with self.bot.get_db() as db:
                try:
                    # executemanyを使って1回の通信で全員分書き込む
                    await db.executemany("""
                        INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET 
                        balance = balance + excluded.balance, total_earned = total_earned + excluded.total_earned
                    """, account_updates)

                    await db.executemany("""
                        INSERT INTO transactions (sender_id, receiver_id, amount, type, batch_id, month_tag, description)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, transaction_inserts)

                    await db.commit()
                except Exception as e:
                    await db.rollback()
                    return await interaction.followup.send(f"❌ DBエラーが発生しました: {e}")
        else:
             return await interaction.followup.send("⚠️ 給与対象者がいませんでした。")

        # ── 4. DM送信 (レート制限対策付き) ──
        sent_dm = 0
        for m, total, matching in payout_data_list:
            try:
                embed = self.create_salary_slip_embed(m, total, matching, month_tag)
                await m.send(embed=embed)
                sent_dm += 1
                # Discord APIのレート制限（BAN）回避のため、5件ごとに1秒休む
                if sent_dm % 5 == 0: 
                    await asyncio.sleep(1) 
            except:
                pass

        await interaction.followup.send(f"💰 **一括支給完了** (ID: `{batch_id}`)\n人数: {count}名 / 総額: {total_payout:,} Stell\n通知送信: {sent_dm}名")
        await self.send_salary_log(interaction, batch_id, total_payout, count, role_summary, now)

    def create_salary_slip_embed(self, member, total, matching, month_tag):
        sorted_matching = sorted(matching, key=lambda x: x[0], reverse=True)
        main_role = sorted_matching[0][1]
        
        embed = discord.Embed(
            title="💰 月給支給のお知らせ",
            description=f"**{month_tag}** の月給が支給されました！",
            color=Color.SUCCESS,
            timestamp=datetime.datetime.now()
        )
        
        embed.add_field(name="💵 支給総額", value=f"**{total:,} Stell**", inline=False)
        
        formula = " + ".join([f"{w:,}" for w, r in sorted_matching])
        embed.add_field(name="🧮 計算式", value=f"{formula} = **{total:,} Stell**", inline=False)
        
        breakdown = "\n".join([f"{i+1}. {r.name}: {w:,} Stell" for i, (w, r) in enumerate(sorted_matching)])
        embed.add_field(name="📊 給与内訳", value=breakdown, inline=False)
        
        embed.add_field(name="🏆 メインロール", value=main_role.name, inline=True)
        embed.add_field(name="🔢 適用ロール数", value=f"{len(matching)}個", inline=True)
        embed.add_field(name="📅 支給月", value=month_tag, inline=True)

        if len(matching) > 1:
            embed.add_field(
                name="⚠️ 複数ロール適用", 
                value="あなたは複数の給与対象ロールを持っているため、全ての給与が合算されて支給されています。", 
                inline=False
            )
        
        embed.set_footer(text="給与計算についてご質問がありましたら管理者にお声がけください")
        return embed

    @app_commands.command(name="給与一覧", description="現在設定されている役職ごとの給与テーブルを表示します")
    async def list_wages(self, interaction: discord.Interaction):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT role_id, amount FROM role_wages ORDER BY amount DESC") as cursor:
                rows = await cursor.fetchall()
        
        if not rows:
            return await interaction.response.send_message("⚠️ 給与設定はまだ登録されていません。", ephemeral=True)
        
        embed = discord.Embed(title="📋 給与テーブル設定一覧", color=Color.TICKET)
        text = ""
        for row in rows:
            role = interaction.guild.get_role(int(row['role_id']))
            role_str = role.mention if role else f"不明なロール(`{row['role_id']}`)"
            text += f"{role_str}: **{row['amount']:,} Stell**\n"
        
        embed.description = text
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="一括給与取り消し", description="【最高神】識別ID(Batch ID)を指定して給与支給を取り消します")
    @has_permission("SUPREME_GOD")
    async def salary_rollback(self, interaction: discord.Interaction, batch_id: str):
        await interaction.response.defer(ephemeral=True)
        
        async with self.bot.get_db() as db:
            async with db.execute("SELECT receiver_id, amount FROM transactions WHERE batch_id = ? AND type = 'SALARY'", (batch_id,)) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                return await interaction.followup.send(f"❌ ID `{batch_id}` の給与データが見つかりません。", ephemeral=True)
            
            try:
                for row in rows:
                    await db.execute("""
                        UPDATE accounts SET balance = balance - ?, total_earned = total_earned - ? 
                        WHERE user_id = ?
                    """, (row['amount'], row['amount'], row['receiver_id']))
                
                await db.execute("DELETE FROM transactions WHERE batch_id = ?", (batch_id,))
                await db.commit()
            except Exception as e:
                await db.rollback()
                logger.error(f"Rollback Error: {e}")
                return await interaction.followup.send("❌ エラーが発生しました。")

        await interaction.followup.send(f"↩️ **ロールバック完了**\nID: `{batch_id}` の支給を回収しました。")

    async def send_salary_log(self, interaction, batch_id, total, count, breakdown, timestamp):
        log_ch_id = None
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'salary_log_id'") as c:
                row = await c.fetchone()
                if row: log_ch_id = int(row['value'])
        
        if not log_ch_id: return
        channel = self.bot.get_channel(log_ch_id)
        if not channel: return

        embed = discord.Embed(title="給与一斉送信ログ", color=Color.STELL, timestamp=timestamp)
        embed.add_field(name="実行者", value=interaction.user.mention, inline=True)
        embed.add_field(name="総額 / 人数", value=f"**{total:,} Stell** / {count}名", inline=True)
        
        breakdown_text = "\n".join([f"✅ {d['mention']}: {d['amount']:,} Stell ({d['count']}名)" for d in breakdown.values()])
        if breakdown_text:
            embed.add_field(name="ロール別内訳", value=breakdown_text, inline=False)
        
        embed.set_footer(text=f"BatchID: {batch_id}")
        await channel.send(embed=embed)

class Jackpot(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.code_price = 5000
        self.pool_addition = 3000   # 5000のうち、金庫に入る額
        self.stella_pocket = 2000   # 5000のうち、消滅する額（インフレ対策）
        self.stella_tax_rate = 0.20 # 当選時のステラの手数料（20%回収）
        self.limit_per_round = 30
        self.max_number = 999
        self.seed_money = 300000    # 初期資金（100万から30万に減額してインフレ抑制）

    async def init_db(self):
        async with self.bot.get_db() as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS lottery_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    number INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS server_config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await db.commit()

    @app_commands.command(name="金庫状況", description="ステラの秘密の金庫の状況と、所持している解除コードを確認します")
    async def status(self, interaction: discord.Interaction):
        await self.init_db()
        
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'jackpot_pool'") as c:
                row = await c.fetchone()
                pool = int(row['value']) if row else self.seed_money

            async with db.execute("SELECT number FROM lottery_tickets WHERE user_id = ? ORDER BY number", (interaction.user.id,)) as c:
                my_codes = await c.fetchall()
                my_numbers = [f"{row['number']:03d}" for row in my_codes]

            async with db.execute("SELECT COUNT(*) as total FROM lottery_tickets") as c:
                sold_count = (await c.fetchone())['total']

        embed = discord.Embed(title="🔐 ステラの秘密の金庫", color=Color.GAMBLE)
        embed.description = (
            "「ふふっ、私の裏金庫が気になるの？ どうせあんたたちには開けられないわよ♡」\n\n"
            "3桁のハッキングコード(000-999)が正解と一致すれば、金庫の中身を強奪！\n"
            "失敗した場合は**全額キャリーオーバー**されます。\n"
        )
        
        embed.add_field(name="💰 現在の保管額", value=f"**{pool:,} Stell**", inline=False)
        embed.add_field(name="💻 発行済みコード数", value=f"{sold_count:,} 個", inline=True)
        embed.add_field(name="📅 ロック解除確率", value="1 / 1000", inline=True)

        if my_numbers:
            code_str = ", ".join(my_numbers)
            if len(code_str) > 500: code_str = code_str[:500] + "..."
            embed.add_field(name=f"🔑 あなたの解除コード ({len(my_numbers)}個)", value=f"`{code_str}`", inline=False)
        else:
            embed.add_field(name="🔑 あなたの解除コード", value="未所持", inline=False)
        
        embed.set_footer(text=f"コード代({self.code_price}S)のうち、{self.stella_pocket}Sはステラのお小遣いとして消滅します")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="ハッキングコード生成", description="金庫の解除コードを生成します (1回 5,000 Stell)")
    @app_commands.describe(amount="生成回数")
    async def buy(self, interaction: discord.Interaction, amount: int):
        if amount <= 0: return await interaction.response.send_message("1回以上指定してください。", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        user = interaction.user
        total_cost = self.code_price * amount
        total_pool_add = self.pool_addition * amount
        total_burn = self.stella_pocket * amount

        async with self.bot.get_db() as db:
            async with db.execute("SELECT COUNT(*) as count FROM lottery_tickets WHERE user_id = ?", (user.id,)) as c:
                current_count = (await c.fetchone())['count']
                if current_count + amount > self.limit_per_round:
                    return await interaction.followup.send(f"ステラ「ちょっと、ガッツきすぎよ！ 上限は {self.limit_per_round}回 までだからね！」\n(残り: {self.limit_per_round - current_count}回)", ephemeral=True)

            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (user.id,)) as c:
                row = await c.fetchone()
                if not row or row['balance'] < total_cost:
                    return await interaction.followup.send("ステラ「…お金ないじゃん。貧乏人は帰って。」", ephemeral=True)

            try:
                # ユーザーからお金を引き落とし
                await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (total_cost, user.id))
                
                # プール追加分のみ金庫へ。残りの burn 分はどこにも足さず「消滅（インフレ対策）」させる
                await db.execute("""
                    INSERT INTO server_config (key, value) VALUES ('jackpot_pool', ?) 
                    ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + ?
                """, (total_pool_add, total_pool_add))

                new_codes = []
                my_numbers = []
                for _ in range(amount):
                    num = random.randint(0, self.max_number)
                    new_codes.append((user.id, num))
                    my_numbers.append(f"{num:03d}")
                
                await db.executemany("INSERT INTO lottery_tickets (user_id, number) VALUES (?, ?)", new_codes)
                await db.commit()

                num_display = ", ".join(my_numbers)
                msg = (
                    f"ステラ「はい、ハッキングコードよ。どうせ当たらないんだから無駄遣いね♡\n"
                    f"（小声）ふふっ、{total_burn:,} Stell は私のお小遣いっと…♪」\n\n"
                    f"✅ **{amount}個** 生成しました！\n獲得コード: `{num_display}`\n"
                    f"(購入代金のうち、金庫に **{total_pool_add:,} S** 追加されました)"
                )
                await interaction.followup.send(msg, ephemeral=True)

            except Exception as e:
                await db.rollback()
                traceback.print_exc()
                await interaction.followup.send("❌ システムエラーが発生しました。", ephemeral=True)

    @app_commands.command(name="金庫解除", description="【管理者】金庫のロック解除処理を実行します")
    @app_commands.describe(panic_release="Trueの場合、発行済みコードの中から強制的に正解を選びます(特大還元祭)")
    @app_commands.default_permissions(administrator=True)
    async def draw(self, interaction: discord.Interaction, panic_release: bool = False):
        await interaction.response.defer()
        
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'jackpot_pool'") as c:
                row = await c.fetchone()
                current_pool = int(row['value']) if row else self.seed_money
                if current_pool < self.seed_money: current_pool = self.seed_money

        winning_number = random.randint(0, self.max_number)
        winners = []
        is_panic = False

        async with self.bot.get_db() as db:
            if panic_release:
                async with db.execute("SELECT user_id, number FROM lottery_tickets") as c:
                    all_sold = await c.fetchall()
                if not all_sold: return await interaction.followup.send("⚠️ コードが一つも生成されていません。")
                
                is_panic = True
                lucky = random.choice(all_sold)
                winning_number = lucky['number']
                winners = [t for t in all_sold if t['number'] == winning_number]
            else:
                async with db.execute("SELECT user_id FROM lottery_tickets WHERE number = ?", (winning_number,)) as c:
                    winners = await c.fetchall()

            winning_str = f"{winning_number:03d}"
            
            embed = discord.Embed(title="🚨 ステラ金庫 ハッキング判定", color=Color.STELL)
            embed.add_field(name="🎯 正解コード", value=f"<h1>**{winning_str}**</h1>", inline=False)

            if len(winners) > 0:
                # 【インフレ対策】ステラの手数料天引き (消滅するお金)
                stella_tax = int(current_pool * self.stella_tax_rate)
                actual_prize_pool = current_pool - stella_tax
                
                prize_per_winner = actual_prize_pool // len(winners)
                winner_mentions = []
                for w in winners:
                    uid = w['user_id']
                    await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (prize_per_winner, uid))
                    winner_mentions.append(f"<@{uid}>")
                
                # プールを初期資金(30万)にリセット
                await db.execute("UPDATE server_config SET value = ? WHERE key = 'jackpot_pool'", (str(self.seed_money),))

                await db.execute("DELETE FROM lottery_tickets")
                await db.commit()

                desc = f"ステラ「う、嘘でしょ！？ 私の金庫が…開けられた！？\n……し、しょーがないわね。ヘソクリにしてた分 {self.stella_tax_rate*100}%({stella_tax:,} S) は私が頂くから！」"
                if is_panic: desc = f"ステラ「ちょ、ちょっとシステムエラー！？ なんで勝手に開いてるのよ！！ 泥棒ー！！\nせ、せめて次の競馬代 {self.stella_tax_rate*100}%({stella_tax:,} S) だけでも確保しなきゃ…！」\n🚨 **パニック・リリース発動！強制放出！** 🚨"
                
                embed.description = f"{desc}\n\n🎉 **{len(winners)}名** のハッカーが金庫破りに成功しました！"
                embed.add_field(name="💰 1人あたりの獲得額", value=f"**{prize_per_winner:,} Stell** (手数料引抜き後)", inline=False)
                
                mentions = " ".join(list(set(winner_mentions)))
                if len(mentions) > 1000: mentions = f"{len(winners)}名の当選者"
                embed.add_field(name="🏆 成功者一覧", value=mentions, inline=False)
                
                embed.set_footer(text=f"金庫の残高はシステムによって{self.seed_money:,} Stellにリセットされました。")
                embed.color = 0xff00ff 

            else:
                await db.execute("DELETE FROM lottery_tickets")
                await db.commit()
                embed.description = "ステラ「あーっはっは！ ざぁこ♡ 誰一人開けられないじゃない！ このお金はぜーんぶ私のものね！」\n\n💀 **金庫破り失敗...**"
                embed.add_field(name="💸 キャリーオーバー", value=f"現在の **{current_pool:,} Stell** は次回に持ち越されます！", inline=False)
                embed.color = 0x2f3136

        await interaction.followup.send(content="@everyone", embed=embed)

# ============================================================
#  Chinchiro Cog  ―  PVP親子対戦 + PVEレイド（vsセスタ）
#
#  【お金の流れ】
#  ■ PVP（/チンチロ開始）
#    ・場所代: 賭け金の5% × 全員 → Burn
#    ・勝敗はPVP（親子間のやりとり）
#    ・JPへの積み立てなし
#
#  ■ PVEレイド（/チンチロレイド）
#    ・場所代: 賭け金の5% × 全員 → Burn
#    ・負け分: 5% → JP積み立て、95% → Burn
#    ・勝者報酬: 役に応じた倍率（最大x2、インフレ抑制設計）
#
#  【導入】古いChinchiroクラスをこれに差し替え。
#          冒頭のヘルパー関数も一緒に貼る。
# ============================================================

# ========== ヘルパー関数 ==========
DICE_EMOJI = {1:"⚀", 2:"⚁", 3:"⚂", 4:"⚃", 5:"⚄", 6:"⚅"}

def dice_str(dice):
    return " ".join(DICE_EMOJI[d] for d in dice)

def judge_roll(dice):
    """
    Returns (role_name, score, mult)
    mult: 5=ピンゾロ / 3=ゾロ目 / 2=シゴロ / None=目あり / -1=ヒフミ / 0=ハチ目
    """
    d = sorted(dice)
    counts = {v: dice.count(v) for v in set(dice)}
    if d == [1,1,1]:         return ("🌟 ピンゾロ！",      100,  5)
    if len(counts) == 1:     return (f"✨ ゾロ目({d[0]})", d[0]*10+50, 3)
    if d == [4,5,6]:         return ("🔥 シゴロ！",         99,   2)
    if d == [1,2,3]:         return ("💀 ヒフミ…",          -1,  -1)
    if 2 in counts.values():
        for v, c in counts.items():
            if c == 1:
                return (f"🎯 目あり({v})", v, None)
    return ("😶 ハチ目", 0, 0)

def roll_until_role(max_tries=3):
    """役が出るまで最大3回。Returns (all_rolls, role_name, score, mult)"""
    all_rolls = []
    role_name, score, mult = "😶 ハチ目", 0, 0
    for _ in range(max_tries):
        dice = [random.randint(1,6) for _ in range(3)]
        all_rolls.append(dice)
        role_name, score, mult = judge_roll(dice)
        if mult != 0:
            break
    return all_rolls, role_name, score, mult

def score_rank(mult, score):
    if mult == 5:    return (5, score)
    if mult == 3:    return (3, score)
    if mult == 2:    return (2, score)
    if mult is None: return (1, score)
    if mult == -1:   return (-1, 0)
    return (0, 0)

def determine_outcome(h_mult, h_score, c_mult, c_score):
    """子から見た勝敗。'child_win'/'host_win'/'draw'"""
    h = score_rank(h_mult, h_score)
    c = score_rank(c_mult, c_score)
    if c > h: return "child_win"
    if c < h: return "host_win"
    return "draw"

def pvp_payout_mult(mult):
    """PVP: 勝ったときの純利益倍率"""
    if mult == 5: return 5
    if mult == 3: return 3
    if mult == 2: return 2
    return 1

def solo_reward_mult(mult):
    """
    PVE一人用: 還元率95%に調整した返却倍率（場所代なし）
    シミュレーション検証済み: 期待値≒95%
    """
    if mult == 5:    return 3.4
    if mult == 3:    return 2.75
    if mult == 2:    return 2.25
    if mult is None: return 1.75
    if mult == -1:   return 1.45
    return 1.0
    
# ================================================================
#   チンチロ セリフ
# ================================================================

CHINCHIRO_LINES = {

    # ── 募集・開始 ──────────────────────────────────────
    "start": [
        "チンチロやるの？……まぁ、アタシが仕切ってあげる。感謝しなよ",
        "どうせ負けるくせに。……でも見てないと心配だから、仕方なく仕切る",
        "場所代はちゃんともらうから。それだけ覚えといて",
    ],
    "join": [
        "また来た。好きにしなよ",
        "参加するの？……来るなとは言ってない",
        "アンタも？……まぁ、いいけど",
    ],

    # ── 1投目実況 ──────────────────────────────────────
    "roll1_hachi": [
        "ハチ目ね。まだあるけど",
        "……ハチ目。次に期待すれば？",
        "1投目ハチ目か。まぁ、よくある",
    ],
    "roll1_good": [
        "おっ、いい目じゃん。続けなよ",
        "……ふーん、悪くない",
        "1投目から良い目。調子いいじゃん",
    ],
    "roll1_hifumi": [
        "ヒフミ。……次頑張って",
        "あー、ヒフミか。まだ2投あるから",
    ],

    # ── 2投目実況 ──────────────────────────────────────
    "roll2_hachi": [
        "……またハチ目。ふふ、ヤバくない？",
        "2投連続ハチ目。次で決めなよ",
        "あー、またハチ目。最後に期待するしかないね",
    ],
    "roll2_reach": [
        "おっ、リーチじゃん！最後決めなよ！",
        "2枚揃った！あと1個！……当たるといいね",
        "リーチ！ねえ、ドキドキする？アタシはしてないけど",
    ],
    "roll2_good": [
        "いい感じじゃん。最後も頼むよ",
        "……悪くない。続けて",
    ],

    # ── 3投目実況 ──────────────────────────────────────
    "roll3_pinzoro": [
        "っな！？ピンゾロ！？……ずるくない？",
        "ピンゾロじゃん！……まぁ、認める。すごかった",
        "え、ピンゾロ！？アタシびっくりしてないけど！？",
    ],
    "roll3_shigoro": [
        "シゴロ！強いじゃん……まぁ",
        "シゴロか。……認めてあげる",
    ],
    "roll3_zorume": [
        "ゾロ目！……やるじゃん",
        "ゾロ目じゃん。……素直にすごいと思う",
    ],
    "roll3_miari": [
        "目あり確定。……まぁよかったじゃん",
        "目あり。悪くない",
    ],
    "roll3_hifumi": [
        "ヒフミ……。次は頑張って",
        "あー、ヒフミか。……気にしないで",
    ],
    "roll3_shonben": [
        "3投ともハチ目。ションベンじゃん♪ ざぁこ〜",
        "ぷぷぷっ！ションベン確定！ざぁこざぁこ♪",
        "あー全部ハチ目！ションベン！アタシ笑いすぎて死ぬ♪",
    ],

    # ── ションベン（1投目飛び）──────────────────────────
    "shonben_fly": [
        "あっ飛んだ♪ ざぁこ確定〜！見た？今の！",
        "えっ飛んだじゃん！！ぷぷっ、ションベンじゃん！ざぁこ！",
        "サイコロ飛んでったじゃん♪ アタシ見てたよ〜！ざぁこざぁこ！",
        "っは！？飛んだ！？ぷぷぷっ！ションベン！最高！ざぁこ！",
    ],

    # ── 結果 ────────────────────────────────────────────
    "child_win": [
        "子が勝った。……まぁ、よくやった",
        "勝ったじゃん。……素直に認める、よかった",
    ],
    "host_sweep": [
        "親の完勝。……負けた人、まぁ次があるから",
        "全滅じゃん。……親が強かっただけで、みんなは悪くなかった",
    ],
    "host_win_partial": [
        "親が勝ち越し。……負けた人お疲れ様",
        "まぁまぁの結果じゃん。……勝った人はよかった",
    ],
    "draw": [
        "引き分け。……賭け金返るし、悪くないんじゃない",
    ],
    "timeout": [
        "……誰も来なかった。別にいいけど",
        "タイムアウト。……待ってたわけじゃないから",
    ],
    "broke": [
        "残高足りてないじゃん。稼いできなよ",
        "お金ないの？……出直してきなよ",
    ],
    "cooldown": [
        "{sec}秒待って。……急かさないで",
        "まだ早い。{sec}秒後にまた来て",
    ],

    # ── PVEソロ専用 ─────────────────────────────────────
    "solo_start": [
        "一人でアタシに挑むの？……面白いじゃん、来なよ",
        "ソロ戦？……アタシが相手してあげる。覚悟はいい？",
        "一人で来たの。……まぁ、相手してあげる",
    ],
    "solo_sesta_roll1": [
        "アタシの1投目……",
        "さて、アタシが振るよ……",
    ],
    "solo_sesta_roll2": [
        "2投目……どうかな",
        "……続けるよ",
    ],
    "solo_sesta_roll3": [
        "最後……",
        "決まるよ……",
    ],
    "solo_player_win": [
        "……負けた。別に、悔しくないけど",
        "やるじゃん。……今日は調子悪かっただけだから",
        "勝ったの？……まぁ、認める。ちゃんと強かった",
    ],
    "solo_sesta_win": [
        "アタシの勝ち。……まぁ、当然だけど",
        "ふふ、負けたじゃん。……次は頑張って",
        "アタシには勝てないよ。……また来ていいけど",
    ],
    "solo_draw": [
        "引き分けか。……まぁ、悪くないんじゃない",
        "引き分け。……賭け金返すよ",
    ],
    "solo_shonben_player": [
        "あっ飛んだ♪ ざぁこ確定〜！見た？今の！",
        "えっ飛んだじゃん！！ぷぷっ、ションベンじゃん！ざぁこ！",
        "サイコロ飛んでったじゃん♪ アタシ見てたよ〜！ざぁこざぁこ！",
    ],
    "solo_shonben_sesta": [
        "あっ……飛んだ。……見なかったことにして",
        "えっ飛んだ！？……今のはノーカンで",
        "っな！？アタシのサイコロが！……これは事故だから！",
    ],
}

def c_line(key: str, **kwargs) -> str:
    lines = CHINCHIRO_LINES.get(key, ["……"])
    line  = random.choice(lines)
    return line.format(**kwargs) if kwargs else line


# ================================================================
#   セッションクラス
# ================================================================

class ChinchiroSession:
    def __init__(self, host, bet, channel_id):
        self.host       = host
        self.bet        = bet
        self.channel_id = channel_id
        self.players    = []
        self.phase      = "recruiting"
        self.started_at = datetime.datetime.now()

#================================================================
#  PVP UI: 募集パネル
# ================================================================

class ChinchiroRecruitView(discord.ui.View):
    def __init__(self, cog: "Chinchiro", session: ChinchiroSession):
        super().__init__(timeout=120)
        self.cog     = cog
        self.session = session

    @discord.ui.button(label="参加する", style=discord.ButtonStyle.primary, emoji="🎲")
    async def join_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        s    = self.session
        user = interaction.user

        if s.phase != "recruiting":
            return await interaction.response.send_message("もう始まってるじゃん", ephemeral=True)
        if user.id == s.host.id:
            return await interaction.response.send_message("アンタが親じゃん", ephemeral=True)
        if any(p.id == user.id for p in s.players):
            return await interaction.response.send_message("もう入ってるじゃん", ephemeral=True)
        if len(s.players) >= 7:
            return await interaction.response.send_message("満員じゃん", ephemeral=True)

        venue_fee = int(s.bet * Chinchiro.VENUE_RATE)
        async with self.cog.bot.get_db() as db:
            bal = await self.cog._get_stell(db, user.id)
        if bal < s.bet + venue_fee:
            return await interaction.response.send_message(
                f"セスタ「{c_line('broke')}」", ephemeral=True
            )

        s.players.append(user)
        await interaction.response.send_message(
            f"✅ {user.mention} が参加！\nセスタ「{c_line('join')}」",
        )
        await self._update_panel(interaction)

    @discord.ui.button(label="開始する", style=discord.ButtonStyle.success, emoji="▶️")
    async def start_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        s = self.session
        if interaction.user.id != s.host.id:
            return await interaction.response.send_message(
                "親だけが開始できるじゃん", ephemeral=True
            )
        if s.phase != "recruiting":
            return await interaction.response.send_message(
                "もう始まってるじゃん", ephemeral=True
            )
        if not s.players:
            return await interaction.response.send_message(
                "セスタ「子が誰もいない。一人でやっても意味ないじゃん」",
                ephemeral=True
            )

        s.phase = "rolling"
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        await self.cog._execute_pvp(interaction, s)

    async def on_timeout(self):
        ch_id = self.session.channel_id
        if ch_id in self.cog.sessions:
            del self.cog.sessions[ch_id]
        for child in self.children:
            child.disabled = True

    async def _update_panel(self, interaction: discord.Interaction):
        s         = self.session
        venue_fee = int(s.bet * Chinchiro.VENUE_RATE)
        embed     = discord.Embed(title="🎲 チンチロ 参加者募集中！", color=Color.GAMBLE)
        embed.description = (
            f"**親:** {s.host.mention}\n"
            f"**賭け金:** {s.bet:,} セスタ　**場所代:** {venue_fee:,} Stell/人（Burn）\n\n"
            f"**参加者（{len(s.players)}人）:** "
            + (", ".join(p.mention for p in s.players) if s.players else "なし")
            + f"\n\n親が **▶️開始する** を押したらスタート！"
        )
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass


# ================================================================
#   Cog: Chinchiro
# ================================================================


class Chinchiro(commands.Cog):

    COOLDOWN_SECONDS = 10
    VENUE_RATE       = 0.03
    SHONBEN_RATE     = 0.03

    BET_CHOICES = [
        app_commands.Choice(name="1000 Stell",     value=1000),
        app_commands.Choice(name="5000 Stell",     value=5000),
        app_commands.Choice(name="10,000 Stell",   value=10000),
        app_commands.Choice(name="30,000 Stell",   value=30000),
        app_commands.Choice(name="50,000 Stell",  value=50000),
        app_commands.Choice(name="100,000 Stell", value=100000),
    ]

    def __init__(self, bot):
        self.bot       = bot
        self.sessions  : dict = {}
        self.cooldowns : dict = {}

    def _check_cd(self, user_id) -> int | None:
        if user_id in self.cooldowns:
            rem = self.COOLDOWN_SECONDS - (
                datetime.datetime.now() - self.cooldowns[user_id]
            ).total_seconds()
            if rem > 0:
                return int(rem) + 1
        return None

    async def _get_stell(self, db, user_id: int) -> int:
        async with db.execute(
            "SELECT balance FROM accounts WHERE user_id = ?", (user_id,)
        ) as c:
            row = await c.fetchone()
        return row["balance"] if row else 0

    async def _add_stell(self, db, user_id: int, amount: int):
        await db.execute("""
            INSERT INTO accounts (user_id, balance, total_earned)
            VALUES (?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance
        """, (user_id, amount))

    async def _sub_stell(self, db, user_id: int, amount: int) -> bool:
        bal = await self._get_stell(db, user_id)
        if bal < amount:
            return False
        await db.execute(
            "UPDATE accounts SET balance = balance - ? WHERE user_id = ?",
            (amount, user_id)
        )
        return True

    # ── /チンチロ ─────────────────────────────────────────
    @app_commands.command(name="チンチロ", description="チンチロの親になってゲームを開始します（Stell）")
    @app_commands.describe(bet="賭け金（Stell）")
    @app_commands.choices(bet=BET_CHOICES)
    async def chinchiro_start(self, interaction: discord.Interaction, bet: int):
        ch_id = interaction.channel_id
        user  = interaction.user

        if ch_id in self.sessions:
            s = self.sessions[ch_id]
            return await interaction.response.send_message(
                f"❌ **{s.host.display_name}** がゲームを開いています。",
                ephemeral=True
            )

        rem = self._check_cd(user.id)
        if rem:
            return await interaction.response.send_message(
                f"セスタ「{c_line('cooldown', sec=rem)}」", ephemeral=True
            )

        venue_fee = int(bet * self.VENUE_RATE)
        async with self.bot.get_db() as db:
            bal = await self._get_stell(db, user.id)
        if bal < bet + venue_fee:
            return await interaction.response.send_message(
                f"セスタ「{c_line('broke')}」", ephemeral=True
            )

        session = ChinchiroSession(host=user, bet=bet, channel_id=ch_id)
        self.sessions[ch_id] = session

        embed = discord.Embed(title="🎲 チンチロ 参加者募集中！", color=Color.GAMBLE)
        embed.description = (
            f"セスタ「{c_line('start')}」\n\n"
            f"**親:** {user.mention}\n"
            f"**賭け金:** {bet:,} Stell　**場所代:** {venue_fee:,} Stell/人（Burn）\n\n"
            f"参加者: なし\n\n"
            f"**参加する** ボタンで子として参加！\n"
            f"最大7人まで / 120秒で自動終了"
        )
        view = ChinchiroRecruitView(self, session)
        await interaction.response.send_message(embed=embed, view=view)

    # ── PVP 戦闘コア ──────────────────────────────────────
    async def _execute_pvp(self, interaction: discord.Interaction, s: ChinchiroSession):
        bet         = s.bet
        venue_fee   = int(bet * self.VENUE_RATE)
        all_members = [s.host] + s.players

        # 残高チェック（Stell）
        broke = []
        async with self.bot.get_db() as db:
            for m in all_members:
                bal = await self._get_stell(db, m.id)
                if bal < bet + venue_fee:
                    broke.append(m)
        if broke:
            s.phase = "recruiting"
            return await interaction.channel.send(
                f"❌ 残高不足: {', '.join(m.display_name for m in broke)}\n"
                f"セスタ「{c_line('broke')}」"
            )

        # 全員からStell引き落とし
        async with self.bot.get_db() as db:
            for m in all_members:
                await self._sub_stell(db, m.id, bet + venue_fee)
            await db.commit()

        total_burn = venue_fee * len(all_members)
        month_tag  = datetime.datetime.now().strftime("%Y-%m")
        num_children = len(s.players)

        # ── 親のサイコロ演出 ──────────────────────────────
        embed = discord.Embed(
            title="🎲 チンチロ スタート！",
            description=f"**親:** {s.host.mention} がサイコロを振ります…",
            color=Color.STOCK
        )
        msg = await interaction.channel.send(embed=embed)

        # 親ションベンチェック
        host_shonben = random.random() < self.SHONBEN_RATE
        if host_shonben:
            await asyncio.sleep(0.8)
            embed.description = (
                f"**親:** {s.host.mention}\n\n"
                f"🎲 {dice_str([random.randint(1,6) for _ in range(3)])} ← 飛んだ！\n\n"
                f"💦 **ションベン！** 親の即負け！\n"
                f"セスタ「{c_line('shonben_fly')}」"
            )
            embed.color = 0x4444ff
            await msg.edit(embed=embed)

            # 子全員に bet×2 返却（Stell）
            async with self.bot.get_db() as db:
                for m in s.players:
                    await self._add_stell(db, m.id, bet * 2)
                await db.commit()

            now = datetime.datetime.now()
            for m in all_members:
                self.cooldowns[m.id] = now
            if s.channel_id in self.sessions:
                del self.sessions[s.channel_id]
            return

        # 通常の親のロール
        h_rolls, h_role, h_score, h_mult = await self._animated_roll(
            msg, embed, s.host, is_host=True
        )

        # ── 子のサイコロ演出 ──────────────────────────────
        results        = {}
        child_shonbens = {}

        for m in s.players:
            shonben = random.random() < self.SHONBEN_RATE
            child_shonbens[m.id] = shonben
            if shonben:
                results[m.id] = ([], "💦 ションベン", -999, -2)
            else:
                rolls, rname, score, mult = await self._animated_roll(
                    msg, embed, m, is_host=False, host_role=h_role
                )
                results[m.id] = (rolls, rname, score, mult)

        # ── 精算（ゼロサム・Stell）────────────────────────
        # C案: 子が勝ったとき払い出し額 = min(bet*役倍率, 親のbet)
        # 参加者が多いほど親のbetプールが増えて高倍率が意味を持つ
        win_members  = []
        lose_members = []
        draw_members = []
        child_lines  = []

        # 親が勝った子から受け取れる額のプール
        host_pool = bet * num_children  # 子全員のbet合計

        async with self.bot.get_db() as db:
            for m in s.players:
                rolls, role_name, score, mult = results[m.id]

                if child_shonbens[m.id]:
                    child_lines.append(
                        f"💦 {m.mention} **ションベン！** 即負け\n"
                        f"セスタ「{c_line('shonben_fly')}」"
                    )
                    lose_members.append(m)
                    continue

                outcome = determine_outcome(h_mult, h_score, mult, score)
                parts   = []
                for i, r in enumerate(rolls):
                    suffix = f"**{role_name}**" if i == len(rolls)-1 else "ハチ目"
                    parts.append(f"　{i+1}投目: {dice_str(r)} {suffix}")
                roll_disp = "\n".join(parts)

                if outcome == "child_win":
                    # 払い出し = bet + min(bet×役倍率, 親のpool残り)
                    raw_win   = bet * pvp_payout_mult(mult)
                    actual_win = min(raw_win, host_pool)
                    host_pool -= actual_win
                    payout    = bet + actual_win
                    await self._add_stell(db, m.id, payout)
                    child_lines.append(
                        f"✅ {m.mention}\n{roll_disp}\n"
                        f"　→ **子の勝ち！** +{actual_win:,} Stell"
                    )
                    win_members.append((m, mult, actual_win))

                elif outcome == "host_win":
                    child_lines.append(
                        f"❌ {m.mention}\n{roll_disp}\n　→ **親の勝ち**"
                    )
                    lose_members.append(m)

                else:
                    await self._add_stell(db, m.id, bet)
                    child_lines.append(
                        f"🟡 {m.mention}\n{roll_disp}\n　→ **引き分け**（返却）"
                    )
                    draw_members.append(m)

            # 親の精算
            # 親の受け取り = 元本 + 負けた子のbet - 勝った子に払った額
            total_won  = sum(w for _, _, w in win_members)
            total_lost = len(lose_members) * bet
            host_return = bet + total_lost - total_won
            if host_return > 0:
                await self._add_stell(db, s.host.id, host_return)

            await db.commit()

        # ── 結果Embed ─────────────────────────────────────
        if not win_members and not draw_members:
            key = "host_sweep"
        elif win_members and not lose_members:
            key = "child_win"
        else:
            key = "host_win_partial"

        h_parts = []
        for i, r in enumerate(h_rolls):
            suffix = f"**{h_role}**" if i == len(h_rolls)-1 else "ハチ目"
            h_parts.append(f"　{i+1}投目: {dice_str(r)} {suffix}")

        result_embed = discord.Embed(
            title="🎲 チンチロ 結果発表！",
            description=f"セスタ「{c_line(key)}」",
            color=Color.GAMBLE
        )
        result_embed.add_field(
            name=f"👑 親: {s.host.display_name}  {h_role}",
            value="\n".join(h_parts),
            inline=False
        )
        for line in child_lines:
            result_embed.add_field(name="\u200b", value=line, inline=False)

        host_profit = total_lost - total_won
        profit_str  = f"+{host_profit:,}" if host_profit >= 0 else f"{host_profit:,}"
        summary = []
        if win_members:  summary.append("✅ 勝ち: " + ", ".join(m.display_name for m, _, _ in win_members))
        if lose_members: summary.append("❌ 負け: " + ", ".join(m.display_name for m in lose_members))
        if draw_members: summary.append("🟡 引き分け: " + ", ".join(m.display_name for m in draw_members))
        summary.append(f"\n👑 親（{s.host.display_name}）収支: **{profit_str} Stell**")
        summary.append(f"🏛️ 場所代Burn: **{total_burn:,} Stell**")

        result_embed.add_field(name="📊 収支", value="\n".join(summary), inline=False)
        result_embed.set_footer(
            text=f"賭け金: {bet:,} Stell | 場所代: {venue_fee:,} Stell/人"
        )
        await msg.edit(embed=result_embed)

        now = datetime.datetime.now()
        for m in all_members:
            self.cooldowns[m.id] = now
        if s.channel_id in self.sessions:
            del self.sessions[s.channel_id]

    # ── サイコロアニメーション ────────────────────────────
    async def _animated_roll(
        self, msg, embed, member, is_host: bool,
        host_role: str = ""
    ):
        label = f"👑 親: {member.display_name}" if is_host else f"🎲 {member.display_name}"
        rolls, role_name, score, mult = roll_until_role()
        all_parts = []

        for i, dice in enumerate(rolls):
            is_last = (i == len(rolls) - 1)
            _, _, tmp_mult = judge_roll(dice)

            # セリフ選択
            if i == 0:
                _, _, m0 = judge_roll(dice)
                if m0 == 0:    selife = c_line("roll1_hachi")
                elif m0 == -1: selife = c_line("roll1_hifumi")
                else:          selife = c_line("roll1_good")
            elif i == 1:
                _, _, m1 = judge_roll(dice)
                # 前の目と合わせてリーチ判定
                prev = rolls[0]
                if m1 == 0:
                    selife = c_line("roll2_hachi")
                elif any(dice.count(v) >= 2 for v in dice):
                    selife = c_line("roll2_reach")
                else:
                    selife = c_line("roll2_good")
            else:
                if mult == 5:      selife = c_line("roll3_pinzoro")
                elif mult == 2:    selife = c_line("roll3_shigoro")
                elif mult == 3:    selife = c_line("roll3_zorume")
                elif mult is None: selife = c_line("roll3_miari")
                elif mult == -1:   selife = c_line("roll3_hifumi")
                else:              selife = c_line("roll3_shonben")

            suffix = f"**{role_name}**" if is_last else "ハチ目"
            all_parts.append(f"　{i+1}投目: {dice_str(dice)} {suffix}")

            embed.description = (
                f"{label}\n\n"
                + "\n".join(all_parts)
                + f"\n\nセスタ「{selife}」"
            )
            await msg.edit(embed=embed)
            await asyncio.sleep(1.0)

        return rolls, role_name, score, mult

    # ── /チンチロ解散 ──────────────────────────────────────
    @app_commands.command(name="チンチロ解散", description="開催中のゲームを解散します")
    async def chinchiro_cancel(self, interaction: discord.Interaction):
        ch_id = interaction.channel_id
        user  = interaction.user

        if ch_id not in self.sessions:
            return await interaction.response.send_message(
                "❌ 開催中のゲームはありません。", ephemeral=True
            )
        s = self.sessions[ch_id]
        if s.host.id != user.id and not user.guild_permissions.administrator:
            return await interaction.response.send_message(
                "セスタ「解散できるのは親だけじゃん」", ephemeral=True
            )
        del self.sessions[ch_id]
        await interaction.response.send_message(
            f"🚫 ゲームを解散しました。\nセスタ「また来てよ。……待ってるから」"
        )

    # ── /チンチロ役一覧 ────────────────────────────────────
    @app_commands.command(name="チンチロ役一覧", description="役と倍率の一覧を確認します")
    async def chinchiro_help(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="📖 チンチロ 役一覧",
            description=f"セスタ「覚えてから来なよ。……まぁ、教えてあげるけど」",
            color=Color.GAMBLE
        )
        embed.add_field(name="🌟 ピンゾロ (1-1-1)", value="最強。PVP: x5倍",        inline=False)
        embed.add_field(name="✨ ゾロ目  (n-n-n)", value="PVP: x3倍",               inline=False)
        embed.add_field(name="🔥 シゴロ  (4-5-6)", value="PVP: x2倍",               inline=False)
        embed.add_field(name="🎯 目あり  (n-n-x)", value="目の数字で勝負。PVP: x1倍", inline=False)
        embed.add_field(name="💀 ヒフミ  (1-2-3)", value="即負け役",                 inline=False)
        embed.add_field(name="😶 ハチ目  (その他)", value="役なし。3回→ションベン",   inline=False)
        embed.add_field(name="💦 ションベン",       value="1投目3%で発動。即負け",     inline=False)
        embed.add_field(
            name="💰 お金の流れ",
            value="場所代3% Burn / JPなし",
            inline=False
        )
        embed.set_footer(text=f"CD: {self.COOLDOWN_SECONDS}秒")
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ── /チンチロソロ ─────────────────────────────────────
    @app_commands.command(name="チンチロソロ", description="セスタと1対1でチンチロ勝負！")
    @app_commands.describe(bet="賭け金（セスタ）")
    @app_commands.choices(bet=[
        app_commands.Choice(name="10 セスタ",  value=10),
        app_commands.Choice(name="50 セスタ",  value=50),
        app_commands.Choice(name="100 セスタ", value=100),
    ])
    async def chinchiro_solo(self, interaction: discord.Interaction, bet: int):
        user      = interaction.user
        cesta_cog = self.bot.get_cog("CestaSystem")

        rem = self._check_cd(user.id)
        if rem:
            return await interaction.response.send_message(
                f"セスタ「{c_line('cooldown', sec=rem)}」", ephemeral=True
            )

# ── 日次プレイ上限チェック ──
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        daily_limit = await _cfg(self.bot, "chinchiro_daily_limit")
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT 1 FROM daily_play_exemptions WHERE user_id=? AND game='chinchiro' AND date=?",
                (user.id, today)
            ) as c:
                exempt = await c.fetchone()
            async with db.execute(
                "SELECT count FROM daily_play_counts WHERE user_id=? AND game='chinchiro' AND date=?",
                (user.id, today)
            ) as c:
                row = await c.fetchone()
            play_count = row["count"] if row else 0
        if not exempt and play_count >= daily_limit:
            return await interaction.response.send_message(
                f"🚫 今日のチンチロ上限（**{daily_limit}回**）に達したよ！また明日ね〜♪",
                ephemeral=True
            )

        venue_fee = int(bet * 0.02)   # ソロは場所代2%
        bal       = await cesta_cog.get_balance(user.id)
        if bal < bet + venue_fee:
            return await interaction.response.send_message(
                f"セスタ「{c_line('broke')}」", ephemeral=True
            )

        # 場所代引き落とし＆プレイカウント記録
        async with self.bot.get_db() as db:
            await cesta_cog.sub_balance(db, user.id, bet + venue_fee)
            newly = await cesta_cog.record_spend(db, user.id, bet + venue_fee)
            await db.execute("""
                INSERT INTO daily_play_counts (user_id, game, date, count)
                VALUES (?, 'chinchiro', ?, 1)
                ON CONFLICT(user_id, game, date) DO UPDATE SET count = count + 1
            """, (user.id, today))
            await db.commit()

        embed = discord.Embed(
            title="🎲 チンチロ ソロ戦！",
            description=(
                f"セスタ「{c_line('solo_start')}」\n\n"
                f"**{user.display_name}** vs **セスタ**\n"
                f"賭け金: **{bet:,} セスタ**"
            ),
            color=Color.GAMBLE
        )
        await interaction.response.defer()
        msg = await interaction.followup.send(embed=embed)


        # ── プレイヤーのションベンチェック ──
        player_shonben = random.random() < self.SHONBEN_RATE

        if player_shonben:
            await asyncio.sleep(0.8)
            embed.description = (
                f"**{user.display_name}** の1投目\n\n"
                f"🎲 {dice_str([random.randint(1,6) for _ in range(3)])} ← 飛んだ！\n\n"
                f"💦 **ションベン！** アンタの即負け！\n"
                f"セスタ「{c_line('solo_shonben_player')}」"
            )
            embed.color = 0x4444ff
            await msg.edit(embed=embed)
            self.cooldowns[user.id] = datetime.datetime.now()
            return

        # ── プレイヤーのロール ──
        p_rolls, p_role, p_score, p_mult = await self._animated_roll(
            msg, embed, user, is_host=False
        )

        # ── セスタのションベンチェック ──
        sesta_shonben = random.random() < self.SHONBEN_RATE

        if sesta_shonben:
            await asyncio.sleep(0.8)
            s_parts = [f"　1投目: {dice_str([random.randint(1,6) for _ in range(3)])} ← 飛んだ！"]
            embed.description = (
                f"👾 セスタの番\n\n"
                + "\n".join(s_parts)
                + f"\n\n💦 **ションベン！** セスタの即負け！\n"
                f"セスタ「{c_line('solo_shonben_sesta')}」"
            )
            embed.color = 0x00ff88
            await msg.edit(embed=embed)

            # プレイヤーに報酬
            reward = int(bet * 2.0)
            async with self.bot.get_db() as db:
                await cesta_cog.add_balance(db, user.id, reward)
                await db.commit()

            new_bal = await cesta_cog.get_balance(user.id)
            result_embed = discord.Embed(
                title="🎲 チンチロ ソロ戦 結果",
                color=Color.SUCCESS
            )
            result_embed.add_field(
                name="💦 セスタ ションベン！",
                value=f"アンタの勝ち！ **+{reward - bet:,} セスタ**",
                inline=False
            )
            result_embed.add_field(name="残高", value=f"{new_bal:,} セスタ", inline=True)
            result_embed.set_footer(text=f"場所代: {venue_fee:,} セスタ Burn")
            await msg.edit(embed=result_embed)
            self.cooldowns[user.id] = datetime.datetime.now()
            return

        # ── セスタのロール演出 ──
        embed.description = f"👾 セスタの番…\nセスタ「{c_line('solo_sesta_roll1')}」"
        await msg.edit(embed=embed)
        await asyncio.sleep(0.8)

        s_rolls    = []
        s_role     = "😶 ハチ目"
        s_score    = 0
        s_mult     = 0
        s_parts    = []

        for i in range(3):
            dice = [random.randint(1, 6) for _ in range(3)]
            s_rolls.append(dice)
            s_role_tmp, s_score_tmp, s_mult_tmp = judge_roll(dice)[0], judge_roll(dice)[1], judge_roll(dice)[2]

            is_last = (i == 2) or (s_mult_tmp != 0)
            suffix  = f"**{s_role_tmp}**" if is_last else "ハチ目"
            s_parts.append(f"　{i+1}投目: {dice_str(dice)} {suffix}")

            if i == 0:   selife = c_line("solo_sesta_roll1")
            elif i == 1: selife = c_line("solo_sesta_roll2")
            else:        selife = c_line("solo_sesta_roll3")

            embed.description = (
                f"👾 セスタの番\n\n"
                + "\n".join(s_parts)
                + f"\nセスタ「{selife}」"
            )
            await msg.edit(embed=embed)
            await asyncio.sleep(1.0)

            if s_mult_tmp != 0:
                s_role  = s_role_tmp
                s_score = s_score_tmp
                s_mult  = s_mult_tmp
                break

        # ── 勝敗判定 ──
        outcome = determine_outcome(s_mult, s_score, p_mult, p_score)

        month_tag = datetime.datetime.now().strftime("%Y-%m")
        payout    = 0

        async with self.bot.get_db() as db:
            if outcome == "child_win":
                reward_mult = solo_reward_mult(p_mult)
                payout      = int(bet * reward_mult)
                logger.info(f"[SOLO DEBUG] p_mult={p_mult}, reward_mult={reward_mult}, bet={bet}, payout={payout}")
                await cesta_cog.add_balance(db, user.id, payout)
            elif outcome == "draw":
                payout = bet
                await cesta_cog.add_balance(db, user.id, payout)
            # 負けは没収のまま
            await db.commit()

        new_bal = await cesta_cog.get_balance(user.id)
        net     = payout - bet

        # ── 結果Embed ──
        if outcome == "child_win":
            color    = discord.Color.green()
            result   = f"✅ **アンタの勝ち！**"
            selife   = c_line("solo_player_win")
        elif outcome == "host_win":
            color    = discord.Color.red()
            result   = f"❌ **セスタの勝ち！**"
            selife   = c_line("solo_sesta_win")
        else:
            color    = discord.Color.yellow()
            result   = f"🟡 **引き分け**"
            selife   = c_line("solo_draw")

        p_parts = []
        for i, r in enumerate(p_rolls):
            suffix = f"**{p_role}**" if i == len(p_rolls)-1 else "ハチ目"
            p_parts.append(f"　{i+1}投目: {dice_str(r)} {suffix}")

        result_embed = discord.Embed(
            title="🎲 チンチロ ソロ戦 結果",
            description=f"{result}\nセスタ「{selife}」",
            color=color
        )
        result_embed.add_field(
            name=f"🎲 {user.display_name}  {p_role}",
            value="\n".join(p_parts),
            inline=False
        )
        result_embed.add_field(
            name=f"👾 セスタ  {s_role}",
            value="\n".join(s_parts),
            inline=False
        )

        net_str = f"+{net:,}" if net >= 0 else f"{net:,}"
        result_embed.add_field(name="損益",   value=f"{net_str} セスタ",  inline=True)
        result_embed.add_field(name="残高",   value=f"{new_bal:,} セスタ", inline=True)
        result_embed.set_footer(text=f"賭け金: {bet:,} セスタ | 場所代: {venue_fee:,} セスタ Burn")

        await msg.edit(embed=result_embed)
        self.cooldowns[user.id] = datetime.datetime.now()


BLACKJACK_LINES = {
    "deal": [
        "えー、またアタシがやるのー？まぁいっか、負けないし♪",
        "来たんだ。……勝てると思ってるなら、お生憎様だけど？",
        "しょうがないなぁ。アタシに勝ちたいなら付き合ってあげる♡",
    ],
    "player_hit": [
        "まだ引くの〜？無謀だぁ♪",
        "ふーん、強気じゃん。バーストしても知らないよ？",
        "あーそっちいくんだ。まぁ好きにしたら〜",
    ],
    "player_stand": [
        "あ、止まるんだ。賢い選択じゃない……かもね♪",
        "スタンドか〜。じゃあアタシの番ね、見てて？",
        "そこで止まるの？ま、どうせアタシが勝つけど〜♡",
    ],
    "player_bust": [
        "あっははは！バーストじゃん、ザコすぎ♪",
        "あらら〜、バーストしちゃった。もっとうまくやってよね",
        "えー、バースト？アタシとやるには早かったかなぁ♡",
    ],
    "sesta_bust": [
        "ちょ……っ！？な、なんでバーストしてんの！？ありえないんだけど！",
        "うそ、バースト！？……これは事故。完全に事故だから！",
        "……バーストした。……見なかったことにして？",
    ],
    "player_win": [
        "……まぁ、今回は負けてあげたってだけだから。勘違いしないでよね",
        "ちょっと！なんで勝ってんの！ズルしてない？してないか……",
        "むぅ……認めてあげる。今回だけだけど♪",
    ],
    "sesta_win": [
        "ふふ〜ん、アタシの勝ち♡ 当然でしょ？",
        "えへへ、やっぱアタシには勝てないよ〜♪",
        "ざーんねん♡ アタシ結構強いんだよね〜",
    ],
    "draw": [
        "引き分け〜？なんか物足りないなぁ",
        "あ〜引き分けか。もうちょっと頑張ってよ、張り合いない♪",
        "引き分けかぁ。……まぁ悪くはないけど、次は負かすから",
    ],
    "blackjack": [
        "ちょ……っ！ブラックジャック！？ずるい！絶対ずるい！",
        "えっ、うそ、なんで！？……お、おめでとう。一応ね♡",
        "ブラックジャック……はぁ、すごいじゃん。認めたくないけど認める",
    ],
}

CARD_SUITS = ["♠", "♥", "♦", "♣"]
CARD_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]

def bj_card_value(rank):
    if rank in ["J", "Q", "K"]: return 10
    if rank == "A": return 11
    return int(rank)

def bj_hand_value(hand):
    total = sum(bj_card_value(r) for r, _ in hand)
    aces = sum(1 for r, _ in hand if r == "A")
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total

def bj_card_str(hand, hide_second=False):
    cards = []
    for i, (r, s) in enumerate(hand):
        if i == 1 and hide_second:
            cards.append("🂠")
        else:
            cards.append(f"{s}{r}")
    return "  ".join(cards)

def bj_new_deck():
    deck = [(r, s) for s in CARD_SUITS for r in CARD_RANKS]
    random.shuffle(deck)
    return deck

def c_line_bj(key):
    lines = BLACKJACK_LINES.get(key, ["……"])
    return random.choice(lines)


class BlackjackView(discord.ui.View):
    def __init__(self, cog, interaction, bet, player_hand, sesta_hand, deck, cesta_cog):
        super().__init__(timeout=60)
        self.cog         = cog
        self.interaction = interaction
        self.bet         = bet
        self.player      = player_hand
        self.sesta       = sesta_hand
        self.deck        = deck
        self.cesta_cog   = cesta_cog
        self.done        = False

    def _embed(self, hide_sesta=True, result_text="", color=Color.GAMBLE):
        p_val = bj_hand_value(self.player)
        s_val = bj_hand_value(self.sesta)
        desc = (
            f"**あなたの手札**: {bj_card_str(self.player)}  `{p_val}`\n"
            f"**セスタの手札**: {bj_card_str(self.sesta, hide_second=hide_sesta)}  "
            f"`{'?' if hide_sesta else s_val}`\n"
        )
        if result_text:
            desc += f"\n{result_text}"
        return discord.Embed(
            title="🃏 ブラックジャック vsセスタ",
            description=desc,
            color=color
        )

    async def _finish(self, interaction):
        if self.done: return
        self.done = True
        self.stop()

        while bj_hand_value(self.sesta) < 17:
            self.sesta.append(self.deck.pop())

        s_val = bj_hand_value(self.sesta)
        p_val = bj_hand_value(self.player)

        if s_val > 21:
            result = f"💥 セスタバースト！\nセスタ「{c_line_bj('sesta_bust')}」"
            payout = self.bet * 2
            color  = discord.Color.green()
        elif p_val > s_val:
            result = f"✅ あなたの勝ち！\nセスタ「{c_line_bj('player_win')}」"
            payout = self.bet * 2
            color  = discord.Color.green()
        elif p_val < s_val:
            result = f"❌ セスタの勝ち\nセスタ「{c_line_bj('sesta_win')}」"
            payout = 0
            color  = discord.Color.red()
        else:
            result = f"🟡 引き分け\nセスタ「{c_line_bj('draw')}」"
            payout = self.bet
            color  = discord.Color.yellow()

        net = payout - self.bet
        result += f"\n\n賭け金: **{self.bet:,} セスタ** | 結果: **{'+' if net >= 0 else ''}{net:,} セスタ**"

        async with self.cog.bot.get_db() as db:
            try:
                if payout > 0:
                    await self.cesta_cog.add_balance(db, interaction.user.id, payout)
                await db.commit()
            except Exception as e:
                await db.rollback()
                logger.error(f"BJ _finish DB error (user={interaction.user.id}): {e}")
                await interaction.response.edit_message(
                    content="❌ 精算処理中にエラーが発生しました。管理者にお問い合わせください。",
                    embed=None, view=None
                )
                return

        embed = self._embed(hide_sesta=False, result_text=result, color=color)
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="ヒット 🃏", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction.user.id:
            return await interaction.response.send_message("あなたのゲームじゃないよ！", ephemeral=True)
        if self.done: return

        self.player.append(self.deck.pop())
        p_val = bj_hand_value(self.player)

        if p_val > 21:
            self.done = True
            self.stop()
            result = f"💥 バースト！\nセスタ「{c_line_bj('player_bust')}」\n\n賭け金: **{self.bet:,} セスタ** | 結果: **-{self.bet:,} セスタ**"
            embed = self._embed(hide_sesta=False, result_text=result, color=Color.DANGER)
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            embed = self._embed()
            embed.set_footer(text=f"セスタ「{c_line_bj('player_hit')}」")
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="スタンド ✋", style=discord.ButtonStyle.secondary)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction.user.id:
            return await interaction.response.send_message("あなたのゲームじゃないよ！", ephemeral=True)
        if self.done: return
        await self._finish(interaction)

    async def on_timeout(self):
        self.stop()


class Blackjack(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="ブラックジャック", description="セスタとブラックジャック勝負！")
    @app_commands.describe(bet="賭け金（セスタ）")
    @app_commands.choices(bet=[
        app_commands.Choice(name="5 セスタ",  value=5),
        app_commands.Choice(name="10 セスタ", value=10),
        app_commands.Choice(name="20 セスタ", value=20),
        app_commands.Choice(name="50 セスタ", value=50),
    ])
    async def blackjack(self, interaction: discord.Interaction, bet: int):
        user      = interaction.user
        cesta_cog = self.bot.get_cog("CestaSystem")

# ── 日次プレイ上限チェック ──
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        daily_limit = await _cfg(self.bot, "slot_daily_limit")
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT 1 FROM daily_play_exemptions WHERE user_id=? AND game='blackjack' AND date=?",
                (user.id, today)
            ) as c:
                exempt = await c.fetchone()
            async with db.execute(
                "SELECT count FROM daily_play_counts WHERE user_id=? AND game='blackjack' AND date=?",
                (user.id, today)
            ) as c:
                row = await c.fetchone()
            play_count = row["count"] if row else 0
        if not exempt and play_count >= daily_limit:
            return await interaction.response.send_message(
                f"🚫 今日のブラックジャック上限（**{daily_limit}回**）に達したよ！また明日ね〜",
                ephemeral=True
            )
        
        bal = await cesta_cog.get_balance(user.id)
        if bal < bet:
            return await interaction.response.send_message(
                f"セスタ「残高が足りないじゃん。」", ephemeral=True
            )

        async with self.bot.get_db() as db:
            await cesta_cog.sub_balance(db, user.id, bet)
            await cesta_cog.record_spend(db, user.id, bet)
            await db.execute("""
                INSERT INTO daily_play_counts (user_id, game, date, count)
                VALUES (?, 'blackjack', ?, 1)
                ON CONFLICT(user_id, game, date) DO UPDATE SET count = count + 1
            """, (user.id, today))
            await db.commit()

        deck        = bj_new_deck()
        player_hand = [deck.pop(), deck.pop()]
        sesta_hand  = [deck.pop(), deck.pop()]

        p_val = bj_hand_value(player_hand)
        s_val = bj_hand_value(sesta_hand)

        if p_val == 21:
            if s_val == 21:
                # 両者BJ → 引き分け、賭け金をそのまま返す
                payout = bet
                async with self.bot.get_db() as db:
                    await cesta_cog.add_balance(db, user.id, payout)
                    await db.commit()
                result = f"🟡 **引き分け（両者ブラックジャック）！**\nセスタ「{c_line_bj('draw')}」\n\n賭け金: **{bet:,} セスタ** | 結果: **±0 セスタ**"
                embed = discord.Embed(title="🃏 ブラックジャック vsセスタ", description=(
                    f"**あなたの手札**: {bj_card_str(player_hand)}  `{p_val}`\n"
                    f"**セスタの手札**: {bj_card_str(sesta_hand)}  `{s_val}`\n\n{result}"
                ), color=Color.STELL)
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            # プレイヤーのみBJ → 2.5倍
            payout = int(bet * 2.5)
            async with self.bot.get_db() as db:
                await cesta_cog.add_balance(db, user.id, payout)
                await db.commit()
            result = f"🌟 **ブラックジャック！**\nセスタ「{c_line_bj('blackjack')}」\n\n賭け金: **{bet:,} セスタ** | 結果: **+{payout - bet:,} セスタ**"
            embed = discord.Embed(title="🃏 ブラックジャック vsセスタ", description=(
                f"**あなたの手札**: {bj_card_str(player_hand)}  `{p_val}`\n"
                f"**セスタの手札**: {bj_card_str(sesta_hand)}  `{s_val}`\n\n{result}"
            ), color=Color.STELL)
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        view  = BlackjackView(self, interaction, bet, player_hand, sesta_hand, deck, cesta_cog)
        embed = view._embed()
        embed.set_footer(text=f"セスタ「{c_line_bj('deal')}」")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ── Grand Opening カウントダウン ──
OPEN_AT = datetime.datetime(2026, 2, 26, 0, 0, 0,
                             tzinfo=datetime.timezone(datetime.timedelta(hours=9)))

def build_countdown_embed(now: datetime.datetime) -> discord.Embed:
    diff = OPEN_AT - now
    if diff.total_seconds() <= 0:
        embed = discord.Embed(
            description=(
                "```\n"
                "  ✦  STELLA  ✦\n\n"
                "   ─── GRAND OPEN ───\n\n"
                "  The stage is now yours.\n"
                "```"
            ),
            color=Color.STOCK
        )
        embed.set_footer(text="STELLA — 2026.02.26 00:00 OPEN")
        return embed

    total_sec = int(diff.total_seconds())
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    bar_len = 20
    filled = int((1 - diff.total_seconds() / (24 * 3600)) * bar_len)
    filled = max(0, min(bar_len, filled))
    bar = "█" * filled + "░" * (bar_len - filled)
    embed = discord.Embed(
        description=(
            "```\n"
            "  ✦  STELLA  ✦\n\n"
            "   ─── GRAND OPENING ───\n\n"
            f"   {h:02d}h  {m:02d}m  {s:02d}s\n\n"
            f"   [{bar}]\n"
            "```"
        ),
        color=Color.STOCK
    )
    open_ts = int(OPEN_AT.timestamp())
    embed.set_footer(text=f"STELLA — Pre-Open  |  <t:{open_ts}:F> OPEN")
    return embed


class Countdown(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._panels: dict[int, int] = {}
        self._opened = False

    def cog_load(self):
        self.update_loop.start()

    def cog_unload(self):
        self.update_loop.cancel()

    @tasks.loop(seconds=60)
    async def update_loop(self):
        if not self._panels:
            return
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        embed = build_countdown_embed(now)
        dead = []
        for msg_id, ch_id in self._panels.items():
            ch = self.bot.get_channel(ch_id)
            if ch is None:
                dead.append(msg_id)
                continue
            try:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(embed=embed)
            except discord.NotFound:
                dead.append(msg_id)
            except Exception:
                pass
        for d in dead:
            del self._panels[d]
        if (OPEN_AT - now).total_seconds() <= 0 and not self._opened:
            self._opened = True
            self.update_loop.stop()

    @update_loop.before_loop
    async def before_loop(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="countdown_panel", description="Grand Openingカウントダウンパネルを投稿")
    @app_commands.default_permissions(administrator=True)
    async def countdown_panel(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        msg = await interaction.channel.send(embed=build_countdown_embed(now))
        self._panels[msg.id] = interaction.channel.id
        await interaction.followup.send("✅ カウントダウンパネルを投稿しました。1分ごと自動更新されます。", ephemeral=True)

    @app_commands.command(name="countdown_clear", description="カウントダウンパネルを削除")
    @app_commands.default_permissions(administrator=True)
    async def countdown_clear(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        count = 0
        for msg_id, ch_id in list(self._panels.items()):
            ch = self.bot.get_channel(ch_id)
            if ch:
                try:
                    msg = await ch.fetch_message(msg_id)
                    await msg.delete()
                    count += 1
                except Exception:
                    pass
        self._panels.clear()
        await interaction.followup.send(f"🗑️ {count}件のパネルを削除しました。", ephemeral=True)
    


        
# ── 色定義 ──
def ansi(text, color_code): return f"\x1b[{color_code}m{text}\x1b[0m"
def gold(t): return ansi(t, "1;33")
def red(t): return ansi(t, "1;31")
def green(t): return ansi(t, "1;32")
def pink(t): return ansi(t, "1;35")
def gray(t): return ansi(t, "1;30")
def blue(t): return ansi(t, "1;34")
def yellow(t): return ansi(t, "1;33")
def white(t): return ansi(t, "1;37")

class Omikuji(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.cost = 300
        
        self.FORTUNES = [
            {"name": "【 大 吉 】", "rate": 4,  "payout": 1500, "color": gold, "msg": "「…へぇ、やるじゃない。今日は私の隣に座る？」"},
            {"name": "【 中 吉 】", "rate": 20, "payout": 500,  "color": green, "msg": "「悪くないわね。調子に乗らない程度に頑張りなさい。」"},
            {"name": "【 小 吉 】", "rate": 20, "payout": 300,  "color": green, "msg": "「普通。損はしてないんだから感謝しなさいよ。」"},
            {"name": "【 末 吉 】", "rate": 20, "payout": 100,  "color": gray,  "msg": "「微妙ね。ま、あんたにはお似合いかも。」"},
            {"name": "【　凶　】", "rate": 25, "payout": 0,    "color": red,   "msg": "「プッ、ざまぁないわね。日頃の行いが悪いんじゃなくって？」"},
            {"name": "【 大 凶 】", "rate": 11, "payout": 0,    "color": red,   "msg": "「あはは！ 最高に無様！ 近寄らないで、不幸が移るわ。」"}
        ]

    @app_commands.command(name="おみくじ", description="ステラちゃんが今日の運勢を占います (1回 300 Stell)")
    async def omikuji(self, interaction: discord.Interaction):
        await interaction.response.defer()
        user = interaction.user

        async with self.bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (user.id,)) as c:
                row = await c.fetchone()
                if not row or row['balance'] < self.cost:
                    return await interaction.followup.send("ステラ「300Stellすら持ってないの？ 帰って。」", ephemeral=True)

            await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (self.cost, user.id))

            rand = random.randint(1, 100)
            current = 0
            result = self.FORTUNES[-1]
            
            for f in self.FORTUNES:
                current += f["rate"]
                if rand <= current:
                    result = f
                    break
            
            payout = result["payout"]
            profit = payout - self.cost
            
            if profit >= 0:
                await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (payout, user.id))
            else:
                if payout > 0:
                    await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (payout, user.id))
                
                loss_amount = abs(profit)
                jp_feed = int(loss_amount * 0.20)
                
                if jp_feed > 0:
                    await db.execute("""
                        INSERT INTO server_config (key, value) VALUES ('jackpot_pool', ?) 
                        ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + ?
                    """, (jp_feed, jp_feed))

            await db.commit()

        embed = discord.Embed(color=Color.DARK)
        if payout >= 500: embed.color = 0xffd700
        elif payout == 0: embed.color = 0xff0000

        frame_color = result["color"]
        draw_txt = (
            f"```ansi\n"
            f"{frame_color('┏━━━━━━━━━━━━━━━┓')}\n"
            f"{frame_color('┃')}   {result['name']}   {frame_color('┃')}\n"
            f"{frame_color('┗━━━━━━━━━━━━━━━┛')}\n"
            f"```"
        )

        res_str = f"**{payout} Stell** (収支: {profit:+d} Stell)"
        if profit < 0:
             res_str += f"\n(💸 負け分の20%はJP賞金へ)"

        embed.description = f"{draw_txt}\n{result['msg']}\n\n{res_str}"
        embed.set_footer(text=f"{user.display_name} の運勢")

        await interaction.followup.send(embed=embed)
        
# ── Cog: VoiceSystem (改良版) ──
class VoiceSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.target_vc_ids = set() 
        self.is_ready_processed = False
        self.locks = {} # ユーザーごとのロック {user_id: asyncio.Lock()}
        self.reward_rate = 50 # 基本レート (Stell/分)
        self.all_join_times = {} # 全VC追跡用 {user_id: join_time}
        self.vc_members: Dict[int, Dict[int, datetime.datetime]] = {}  # 縁追跡用 {channel_id: {user_id: join_time}}

    def get_lock(self, user_id):
        if user_id not in self.locks:
            self.locks[user_id] = asyncio.Lock()
        return self.locks[user_id]

    async def reload_targets(self):
        try:
            async with self.bot.get_db() as db:
                # 報酬対象VCの読み込み
                async with db.execute("SELECT channel_id FROM reward_channels") as cursor:
                    rows = await cursor.fetchall()
                self.target_vc_ids = {row['channel_id'] for row in rows}
                
                # 報酬レートの読み込み (設定がなければデフォルト50)
                async with db.execute("SELECT value FROM server_config WHERE key = 'vc_reward_rate'") as cursor:
                    row = await cursor.fetchone()
                    if row: self.reward_rate = int(row['value'])
            
            logger.info(f"Loaded {len(self.target_vc_ids)} reward VCs. Rate: {self.reward_rate}/min")
        except Exception as e:
            logger.error(f"Failed to load voice config: {e}")

    # インフレ対策コマンド: 報酬レートの変更
    @app_commands.command(name="vc報酬レート設定", description="VC報酬の基本レート(1分あたり)を変更します")
    @has_permission("ADMIN")
    async def set_vc_rate(self, interaction: discord.Interaction, amount: int):
        if amount < 0: return await interaction.response.send_message("❌ 0以上にしてください。", ephemeral=True)
        
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('vc_reward_rate', ?)", (str(amount),))
            await db.commit()
        
        self.reward_rate = amount
        await interaction.response.send_message(f"✅ VC報酬レートを **{amount} Stell / 分** に変更しました。\n(インフレ時は下げ、キャンペーン時は上げてください)", ephemeral=True)

    def is_active(self, state):
        # 判定強化: サーバーミュート/自己ミュート/サーバー拒否/自己拒否 すべてチェック
        return (
            state and 
            state.channel and 
            state.channel.id in self.target_vc_ids and  
            not state.self_deaf and not state.deaf and # 聞けない状態はNG
            not state.self_mute and not state.mute
        )

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        if member.bot: return
        
        # ロックを取得して同時実行を防ぐ
        async with self.get_lock(member.id):
            now = datetime.datetime.now()
            was_active, is_now_active = self.is_active(before), self.is_active(after)

            # 入室 (または条件達成)
            if not was_active and is_now_active:
                try:
                    async with self.bot.get_db() as db:
                        await db.execute(
                            "INSERT OR REPLACE INTO voice_tracking (user_id, join_time) VALUES (?,?)", 
                            (member.id, now.isoformat())
                        )
                        await db.commit()
                except Exception as e:
                    logger.error(f"Voice Tracking Error: {e}")

            # 退室 (または条件未達)
            elif was_active and not is_now_active:
                await self._process_reward(member, now)

        # ── 全VC在室時間追跡（ランキング用） ──
        # 新しいVCに入った（または別VCに移動した）
        if after.channel and (not before.channel or before.channel.id != after.channel.id):
            self.all_join_times[member.id] = now
            # 縁追跡: 新チャンネルに入室記録
            ch_id = after.channel.id
            if ch_id not in self.vc_members:
                self.vc_members[ch_id] = {}
            self.vc_members[ch_id][member.id] = now
            # 古いチャンネルから退出記録
            if before.channel:
                old_ch_id = before.channel.id
                await self._update_bonds(member.id, old_ch_id, now)
                if old_ch_id in self.vc_members:
                    self.vc_members[old_ch_id].pop(member.id, None)

        # VCから完全に退出した
        elif not after.channel and before.channel:
            # 縁追跡: 退出処理
            old_ch_id = before.channel.id
            await self._update_bonds(member.id, old_ch_id, now)
            if old_ch_id in self.vc_members:
                self.vc_members[old_ch_id].pop(member.id, None)
            if member.id in self.all_join_times:
                join_time = self.all_join_times.pop(member.id)
                elapsed = int((now - join_time).total_seconds())
                if elapsed > 0:
                    month_tag = now.strftime("%Y-%m")
                    try:
                        vc_xp = int(elapsed / 60) * 10  # 1分10XP
                        async with self.bot.get_db() as db:
                            await db.execute(
                                "INSERT OR IGNORE INTO vc_rank_stats (user_id, month, total_seconds) VALUES (?, ?, 0)",
                                (member.id, month_tag)
                            )
                            await db.execute(
                                "UPDATE vc_rank_stats SET total_seconds = total_seconds + ? WHERE user_id = ? AND month = ?",
                                (elapsed, member.id, month_tag)
                            )
                            # レベルXP加算
                            if vc_xp > 0:
                                await db.execute(
                                    "INSERT OR IGNORE INTO user_levels (user_id) VALUES (?)", (member.id,)
                                )
                                await db.execute(
                                    "UPDATE user_levels SET xp = xp + ?, total_vc_seconds = total_vc_seconds + ? WHERE user_id = ?",
                                    (vc_xp, elapsed, member.id)
                                )
                                # レベル更新
                                async with db.execute("SELECT xp FROM user_levels WHERE user_id = ?", (member.id,)) as c:
                                    row = await c.fetchone()
                                if row:
                                    new_level = RankingSystem.calc_level(row['xp'])
                                    await db.execute("UPDATE user_levels SET level = ? WHERE user_id = ?", (new_level, member.id))
                            await db.commit()
                    except Exception as e:
                        logger.error(f"VC Rank Stats Error: {e}")

    async def _process_reward(self, member_or_id, now):
        user_id = member_or_id.id if isinstance(member_or_id, discord.Member) else member_or_id
        member  = member_or_id if isinstance(member_or_id, discord.Member) else None

        try:
            async with self.bot.get_db() as db:
                async with db.execute("SELECT join_time FROM voice_tracking WHERE user_id =?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                if not row: return

                try:
                    join_time = datetime.datetime.fromisoformat(row['join_time'])
                    sec = int((now - join_time).total_seconds())

                    if sec < 60:
                        reward = 0
                    else:
                        reward = int(self.reward_rate * (sec / 60))
                        # 3人以上いるVCなら2倍ボーナス
                        if member and member.voice and member.voice.channel:
                            vc_members = [m for m in member.voice.channel.members if not m.bot]
                            if len(vc_members) >= 3:
                                reward *= 2

                    if reward > 0:
                        month_tag = now.strftime("%Y-%m")

                        await db.execute("INSERT OR IGNORE INTO accounts (user_id, balance, total_earned) VALUES (0, 0, 0)")
                        await db.execute("INSERT OR IGNORE INTO accounts (user_id, balance, total_earned) VALUES (?, 0, 0)", (user_id,))

                        await db.execute(
                            "UPDATE accounts SET balance = balance +?, total_earned = total_earned +? WHERE user_id =?",
                            (reward, reward, user_id)
                        )

                        await db.execute(
                            "INSERT OR IGNORE INTO voice_stats (user_id, month, total_seconds) VALUES (?, ?, 0)",
                            (user_id, month_tag)
                        )
                        await db.execute(
                            "UPDATE voice_stats SET total_seconds = total_seconds + ? WHERE user_id = ? AND month = ?",
                            (sec, user_id, month_tag)
                        )
                    await db.execute("DELETE FROM voice_tracking WHERE user_id = ?", (user_id,))
                    await db.commit()

                except Exception as db_err:
                    await db.rollback()
                    raise db_err

        except Exception as e:
            logger.error(f"Voice Reward Process Error [{user_id}]: {e}")
    @commands.Cog.listener()
    async def on_ready(self):
        if self.is_ready_processed: return
        self.is_ready_processed = True
        await self.reload_targets()

    async def _update_bonds(self, user_id: int, channel_id: int, now: datetime.datetime):
        """退出時に同じVCにいた全員との縁を更新"""
        if channel_id not in self.vc_members:
            return
        others = {uid: jt for uid, jt in self.vc_members[channel_id].items() if uid != user_id}
        if not others:
            return

        MALE_ROLE   = 1471473616406446120
        FEMALE_ROLE = 1471473863744552992

        # ランク定義 {(同性か異性か): [(必要秒数, ランク名)]}
        RANKS_SAME = [
            (5*3600,   "◆ なんか知ってる人"),
            (20*3600,  "◆◆ まあ友達"),
            (50*3600,  "◆◆◆ 切っても切れないやつ"),
            (100*3600, "✦ 呪いみたいなもん"),
            (200*3600, "__SELECT__"),
        ]
        RANKS_DIFF = [
            (5*3600,   "◆ なんか知ってる人"),
            (20*3600,  "◆◆ まあ友達"),
            (50*3600,  "◆◆◆ 居心地いい人"),
            (100*3600, "✦ うまく説明できない人"),
            (200*3600, "__SELECT__"),
        ]

        def get_rank(total_sec, rank_list):
            current = ""
            for threshold, name in rank_list:
                if total_sec >= threshold:
                    current = name
            return current

        guild = self.bot.guilds[0] if self.bot.guilds else None

        try:
            async with self.bot.get_db() as db:
                for other_id, other_join in others.items():
                    self_join  = self.vc_members[channel_id].get(user_id, now)
                    overlap_start = max(self_join, other_join)
                    elapsed = max(0, int((now - overlap_start).total_seconds()))
                    if elapsed < 60:
                        continue

                    ua, ub = (user_id, other_id) if user_id < other_id else (other_id, user_id)

                    await db.execute(
                        "INSERT OR IGNORE INTO bonds (user_a, user_b, total_seconds, rank) VALUES (?, ?, 0, '')",
                        (ua, ub)
                    )
                    await db.execute(
                        "UPDATE bonds SET total_seconds = total_seconds + ? WHERE user_a = ? AND user_b = ?",
                        (elapsed, ua, ub)
                    )
                    async with db.execute("SELECT total_seconds, rank FROM bonds WHERE user_a = ? AND user_b = ?", (ua, ub)) as c:
                        bond = await c.fetchone()
                    if not bond:
                        continue

                    total_sec  = bond['total_seconds']
                    old_rank   = bond['rank']

                    # 性別判定
                    is_same = True
                    if guild:
                        ma = guild.get_member(ua)
                        mb = guild.get_member(ub)
                        if ma and mb:
                            a_roles = {r.id for r in ma.roles}
                            b_roles = {r.id for r in mb.roles}
                            a_male   = MALE_ROLE   in a_roles
                            a_female = FEMALE_ROLE in a_roles
                            b_male   = MALE_ROLE   in b_roles
                            b_female = FEMALE_ROLE in b_roles
                            if (a_male and b_female) or (a_female and b_male):
                                is_same = False

                    rank_list = RANKS_SAME if is_same else RANKS_DIFF
                    new_rank  = get_rank(total_sec, rank_list)

                    if new_rank and new_rank != old_rank:
                        await db.execute(
                            "UPDATE bonds SET rank = ? WHERE user_a = ? AND user_b = ?",
                            (new_rank, ua, ub)
                        )

                        # 100h達成でDM通知
                        if new_rank == "✦ 呪いみたいなもん" or new_rank == "✦ うまく説明できない人":
                            for uid in [ua, ub]:
                                other_uid = ub if uid == ua else ua
                                try:
                                    user_obj  = guild.get_member(uid) if guild else None
                                    other_obj = guild.get_member(other_uid) if guild else None
                                    if user_obj and other_obj:
                                        embed = discord.Embed(
                                            title="✦ 新しい縁のランクに到達しました",
                                            description=f"**{other_obj.display_name}** との累計VC時間が **100時間** を超えました。\n\n**{new_rank}**",
                                            color=Color.DARK
                                        )
                                        embed.set_thumbnail(url=other_obj.display_avatar.url)
                                        embed.set_footer(text="200時間に達すると、関係の名前を選べるようになります。")
                                        await user_obj.send(embed=embed)
                                except Exception:
                                    pass

                        # 200h達成でDM通知＋選択ボタン
                        if new_rank == "__SELECT__":
                            for uid in [ua, ub]:
                                other_uid = ub if uid == ua else ua
                                try:
                                    user_obj  = guild.get_member(uid) if guild else None
                                    other_obj = guild.get_member(other_uid) if guild else None
                                    if user_obj and other_obj:
                                        embed = discord.Embed(
                                            title="― 200時間 ―",
                                            description=f"**{other_obj.display_name}** との時間が **200時間** を超えました。\nこの関係に、名前をつけてください。",
                                            color=Color.DARK
                                        )
                                        embed.set_thumbnail(url=other_obj.display_avatar.url)
                                        view = BondSelectView(ua, ub, is_same)
                                        await user_obj.send(embed=embed, view=view)
                                except Exception:
                                    pass

                await db.commit()
        except Exception as e:
            logger.error(f"Bond Update Error: {e}")


class BondSelectView(discord.ui.View):
    """200h達成時の関係名選択ビュー"""
    def __init__(self, user_a: int, user_b: int, is_same: bool):
        super().__init__(timeout=86400)  # 24時間
        self.user_a  = user_a
        self.user_b  = user_b
        self.is_same = is_same

        if is_same:
            choices = [
                ("[ I ]  このまま墓まで持ってく",   "このまま墓まで持ってく"),
                ("[ II ]  お互い迷惑かけあってる",   "お互い迷惑かけあってる"),
                ("[ III ]  言わなくてもわかるやつ",  "言わなくてもわかるやつ"),
            ]
        else:
            choices = [
                ("[ I ]  たぶんずっと友達",        "たぶんずっと友達"),
                ("[ II ]  名前つけたくない関係",    "名前つけたくない関係"),
                ("[ III ]  いなくなったら困る人",   "いなくなったら困る人"),
            ]

        for label, value in choices:
            btn = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary)
            btn.callback = self._make_callback(value)
            self.add_item(btn)

    def _make_callback(self, chosen: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id not in [self.user_a, self.user_b]:
                return await interaction.response.send_message("これはあなた宛のメッセージではありません。", ephemeral=True)
            ua, ub = self.user_a, self.user_b
            async with interaction.client.get_db() as db:
                await db.execute(
                    "UPDATE bonds SET rank = ? WHERE user_a = ? AND user_b = ?",
                    (chosen, ua, ub)
                )
                await db.commit()
            embed = discord.Embed(
                description=f"**{chosen}**\n\nこの関係の名前が決まりました。",
                color=Color.DARK
            )
            await interaction.response.edit_message(embed=embed, view=None)
        return callback


class VoiceHistory(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="スタジオ記録", description="今月の報酬VC累計滞在時間を確認します")
    @app_commands.describe(
        member="確認したいユーザー（省略すると自分）",
        role="このロールを持つ全員の一覧を表示（管理者専用）"
    )
    async def vc_history(
        self,
        interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        role: Optional[discord.Role] = None
    ):
        await interaction.response.defer(ephemeral=True)

        current_month = datetime.datetime.now().strftime("%Y-%m")
        is_admin = await interaction.client.is_owner(interaction.user) or any(
            r.id in interaction.client.config.admin_roles and
            interaction.client.config.admin_roles[r.id] in ["SUPREME_GOD", "GODDESS"]
            for r in interaction.user.roles
        )

        # ── ロール指定（管理者専用） ──
        if role is not None:
            if not is_admin:
                return await interaction.followup.send("❌ ロール指定は管理者のみ使用できます。", ephemeral=True)

            targets = [m for m in role.members if not m.bot]
            if not targets:
                return await interaction.followup.send(f"❌ {role.mention} にメンバーがいません。", ephemeral=True)

            async with self.bot.get_db() as db:
                async with db.execute(
                    "SELECT user_id, total_seconds FROM voice_stats WHERE month = ?",
                    (current_month,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    vc_data = {r['user_id']: r['total_seconds'] for r in rows}

            # 時間順にソート
            results = sorted(
                [(m, vc_data.get(m.id, 0)) for m in targets],
                key=lambda x: x[1],
                reverse=True
            )

            embed = discord.Embed(
                title=f"📊 VC滞在記録一覧 ({current_month})",
                description=f"ロール: {role.mention} ({len(targets)}名)",
                color=Color.DARK
            )

            lines = []
            for i, (m, sec) in enumerate(results):
                h = sec // 3600
                mins = (sec % 3600) // 60
                rank = f"`{i+1}.`"
                lines.append(f"{rank} **{m.display_name}** ── {h}時間 {mins}分")

            # embedの文字数制限対策で分割
            chunk = ""
            for line in lines:
                if len(chunk) + len(line) > 1000:
                    embed.add_field(name="\u200b", value=chunk, inline=False)
                    chunk = ""
                chunk += line + "\n"
            if chunk:
                embed.add_field(name="\u200b", value=chunk, inline=False)

            embed.set_footer(text=f"― {interaction.user.display_name}")
            return await interaction.followup.send(embed=embed, ephemeral=True)

        # ── ユーザー個別 ──
        # 他人を見ようとしたら管理者チェック
        target = member or interaction.user
        if target.id != interaction.user.id and not is_admin:
            return await interaction.followup.send("❌ 他のユーザーの記録を見る権限がありません。", ephemeral=True)

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT total_seconds FROM voice_stats WHERE user_id = ? AND month = ?",
                (target.id, current_month)
            ) as cursor:
                row = await cursor.fetchone()
                total_seconds = row['total_seconds'] if row else 0

        h = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        sec = total_seconds % 60

        embed = discord.Embed(
            title=f"🎙️ VC滞在記録 ({current_month})",
            color=Color.DARK
        )
        embed.set_author(name=target.display_name, icon_url=target.display_avatar.url)
        embed.add_field(name="⏱️ 今月の累計", value=f"**{h}時間 {mins}分 {sec}秒**", inline=False)
        embed.add_field(name="📐 合計秒数", value=f"{total_seconds:,} 秒", inline=True)
        embed.set_footer(text=f"― {interaction.user.display_name}")

        await interaction.followup.send(embed=embed, ephemeral=True)


from typing import Optional

_CFG_DEFAULTS = {
    "cesta_rate":          10000,
    "cesta_daily":         5,
    "cesta_daily_buy_cap": 50,
    "slot_daily_limit":    10,
    "slot_bigwin_cd":      30,
}

async def _cfg(bot, key: str) -> int:
    async with bot.get_db() as db:
        async with db.execute(
            "SELECT value FROM server_config WHERE key = ?", (key,)
        ) as c:
            row = await c.fetchone()
    return int(row["value"]) if row else _CFG_DEFAULTS[key]


class CestaSystem(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    async def get_balance(self, user_id: int) -> int:
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT balance FROM cesta_wallets WHERE user_id = ?", (user_id,)
            ) as c:
                row = await c.fetchone()
        return row["balance"] if row else 0

    async def add_balance(self, db, user_id: int, amount: int):
        await db.execute("""
            INSERT INTO cesta_wallets (user_id, balance) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance
        """, (user_id, amount))

    async def sub_balance(self, db, user_id: int, amount: int) -> bool:
        # 同一トランザクション内で残高チェック＋引き落としを行う（競合防止）
        async with db.execute(
            "SELECT balance FROM cesta_wallets WHERE user_id = ?", (user_id,)
        ) as c:
            row = await c.fetchone()
        bal = row["balance"] if row else 0
        if bal < amount:
            return False
        await db.execute(
            "UPDATE cesta_wallets SET balance = balance - ? WHERE user_id = ?",
            (amount, user_id)
        )
        return True

    @app_commands.command(name="セスタ残高", description="セスタコインの残高を確認します")
    async def cesta_balance(self, interaction: discord.Interaction):
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT balance FROM cesta_wallets WHERE user_id = ?", (interaction.user.id,)
            ) as c:
                row = await c.fetchone()
            bal = row["balance"] if row else 0

            async with db.execute(
                "SELECT balance FROM accounts WHERE user_id = ?", (interaction.user.id,)
            ) as c:
                row = await c.fetchone()
            stell_bal = row["balance"] if row else 0

            async with db.execute(
                "SELECT value FROM server_config WHERE key = 'cesta_rate'"
            ) as c:
                row = await c.fetchone()
            rate = int(row["value"]) if row else 10000

        embed = discord.Embed(title="🎰 セスタコイン残高", color=Color.CESTA)
        embed.add_field(name="💜 セスタ", value=f"**{bal:,} セスタ**", inline=True)
        embed.add_field(name="💰 Stell",  value=f"{stell_bal:,} Stell", inline=True)
        embed.add_field(name="変換レート", value=f"{rate:,} Stell = 1 セスタ", inline=False)
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="セスタデイリー", description="本日のセスタコインを受け取ります（1日1回）")
    async def cesta_daily(self, interaction: discord.Interaction):
        today   = datetime.datetime.now().strftime("%Y-%m-%d")
        user_id = interaction.user.id
        daily_amt = await _cfg(self.bot, "cesta_daily")

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT last_claim FROM cesta_daily_claims WHERE user_id = ?", (user_id,)
            ) as c:
                row = await c.fetchone()

            if row and row["last_claim"] == today:
                return await interaction.response.send_message(
                    "⏳ 今日のデイリーはもう受け取ったよ！また明日ね♪",
                    ephemeral=True
                )

            await self.add_balance(db, user_id, daily_amt)
            await db.execute("""
                INSERT INTO cesta_daily_claims (user_id, last_claim) VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_claim = excluded.last_claim
            """, (user_id, today))
            await db.commit()

        new_bal = await self.get_balance(user_id)
        embed = discord.Embed(
            title="🎁 デイリーセスタ受け取り完了！",
            description=f"**+{daily_amt} セスタ** をゲット！\n残高: **{new_bal:,} セスタ**",
            color=Color.CESTA
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="セスタ購入", description="Stellを使ってセスタコインを購入します")
    @app_commands.describe(amount="購入するセスタ量")
    async def cesta_buy(self, interaction: discord.Interaction, amount: int):
        if amount <= 0:
            return await interaction.response.send_message(
                "❌ 1以上の数を指定してね。", ephemeral=True
            )

        today   = datetime.datetime.now().strftime("%Y-%m-%d")
        user_id = interaction.user.id
        rate    = await _cfg(self.bot, "cesta_rate")
        buy_cap = await _cfg(self.bot, "cesta_daily_buy_cap")
        cost    = amount * rate

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT amount FROM cesta_daily_purchases WHERE user_id = ? AND date = ?",
                (user_id, today)
            ) as c:
                pr = await c.fetchone()
        today_bought = pr["amount"] if pr else 0

        if today_bought + amount > buy_cap:
            remaining = buy_cap - today_bought
            return await interaction.response.send_message(
                f"⚠️ 本日の購入上限は **{buy_cap} セスタ** です。\n"
                f"今日あと **{remaining} セスタ** まで購入できます。",
                ephemeral=True
            )

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT balance FROM accounts WHERE user_id = ?", (user_id,)
            ) as c:
                sr = await c.fetchone()
        stell_bal = sr["balance"] if sr else 0

        if stell_bal < cost:
            return await interaction.response.send_message(
                f"❌ Stellが不足しています。\n"
                f"必要: **{cost:,} Stell** / 所持: **{stell_bal:,} Stell**",
                ephemeral=True
            )

        month_tag = datetime.datetime.now().strftime("%Y-%m")
        async with self.bot.get_db() as db:
            await db.execute(
                "UPDATE accounts SET balance = balance - ? WHERE user_id = ?",
                (cost, user_id)
            )
            await self.add_balance(db, user_id, amount)
            await db.execute("""
                INSERT INTO cesta_daily_purchases (user_id, date, amount) VALUES (?, ?, ?)
                ON CONFLICT(user_id, date) DO UPDATE SET amount = amount + excluded.amount
            """, (user_id, today, amount))
            await db.execute("""
                INSERT INTO transactions
                    (sender_id, receiver_id, amount, type, description, month_tag)
                VALUES (?, 0, ?, 'CESTA_BUY', ?, ?)
            """, (user_id, cost, f"セスタ購入 {amount}セスタ", month_tag))
            await db.commit()

        new_cesta = await self.get_balance(user_id)
        embed = discord.Embed(
            title="💜 セスタ購入完了",
            description=(
                f"**{amount:,} セスタ** を購入しました！\n"
                f"-{cost:,} Stell\n"
                f"セスタ残高: **{new_cesta:,} セスタ**"
            ),
            color=Color.CESTA
        )
        embed.set_footer(text=f"本日の購入合計: {today_bought + amount}/{buy_cap} セスタ")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="セスタ設定", description="【管理者】セスタ・ブラックジャック・チンチロの各種設定を変更します")
    @app_commands.describe(
        cesta_rate="Stell→セスタ変換レート (N Stell = 1 セスタ)",
        cesta_daily="デイリー配布量（セスタ）",
        cesta_daily_buy_cap="1日の購入上限（セスタ）",
        slot_daily_limit="ブラックジャック1日プレイ上限（回）",
        chinchiro_daily_limit="チンチロソロ1日プレイ上限（回）",
    )
    @has_permission("SUPREME_GOD")
    async def cesta_config(
        self,
        interaction: discord.Interaction,
        cesta_rate:            Optional[int] = None,
        cesta_daily:           Optional[int] = None,
        cesta_daily_buy_cap:   Optional[int] = None,
        slot_daily_limit:      Optional[int] = None,
        chinchiro_daily_limit: Optional[int] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        updates = {
            "cesta_rate":            cesta_rate,
            "cesta_daily":           cesta_daily,
            "cesta_daily_buy_cap":   cesta_daily_buy_cap,
            "slot_daily_limit":      slot_daily_limit,
            "chinchiro_daily_limit": chinchiro_daily_limit,
        }
        
        changed = {k: v for k, v in updates.items() if v is not None}
        if not changed:
            return await interaction.followup.send(
                "⚠️ 変更する項目を1つ以上指定してください。", ephemeral=True
            )
        async with self.bot.get_db() as db:
            for k, v in changed.items():
                await db.execute(
                    "INSERT OR REPLACE INTO server_config (key, value) VALUES (?, ?)",
                    (k, str(v))
                )
            await db.commit()
        lines = "\n".join(f"• **{k}** → `{v}`" for k, v in changed.items())
        await interaction.followup.send(f"✅ 設定を更新しました:\n{lines}", ephemeral=True)

    @app_commands.command(name="セスタ付与", description="【管理者】指定ユーザーにセスタを付与します")
    @app_commands.describe(user="対象ユーザー", amount="付与量")
    @has_permission("SUPREME_GOD")
    async def cesta_grant(
        self, interaction: discord.Interaction, user: discord.Member, amount: int
    ):
        if amount <= 0:
            return await interaction.response.send_message(
                "❌ 1以上を指定してください。", ephemeral=True
            )
        async with self.bot.get_db() as db:
            await self.add_balance(db, user.id, amount)
            await db.commit()
        new_bal = await self.get_balance(user.id)
        await interaction.response.send_message(
            f"✅ {user.mention} に **{amount:,} セスタ** を付与しました。\n"
            f"残高: {new_bal:,} セスタ",
            ephemeral=True
        )
# ── 累計消費を記録してバッジチェック ──────────────────
    async def record_spend(self, db, user_id: int, amount: int):
        """
        セスタ消費時に呼ぶ。累計更新＋バッジ自動付与チェック。
        スロット・チンチロの消費処理内で await cesta_cog.record_spend(db, user_id, bet) を呼ぶだけでOK。
        """
        await db.execute("""
            INSERT INTO cesta_spent (user_id, total_spent) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET total_spent = total_spent + excluded.total_spent
        """, (user_id, amount))

        # 現在の累計取得
        async with db.execute(
            "SELECT total_spent FROM cesta_spent WHERE user_id = ?", (user_id,)
        ) as c:
            row = await c.fetchone()
        total = row["total_spent"] if row else 0

        # 閾値取得
        async with db.execute(
            "SELECT badge_id, threshold FROM cesta_badge_thresholds ORDER BY threshold ASC"
        ) as c:
            thresholds = await c.fetchall()

        newly_granted = []
        for t in thresholds:
            badge_id  = t["badge_id"]
            threshold = t["threshold"]
            if total >= threshold:
                # 未取得なら付与
                async with db.execute(
                    "SELECT 1 FROM cesta_badges WHERE user_id = ? AND badge_id = ?",
                    (user_id, badge_id)
                ) as c:
                    has = await c.fetchone()
                if not has:
                    now_str = datetime.datetime.now().isoformat()
                    await db.execute(
                        "INSERT INTO cesta_badges (user_id, badge_id, granted_at) VALUES (?, ?, ?)",
                        (user_id, badge_id, now_str)
                    )
                    newly_granted.append(badge_id)

        return newly_granted   # 新たに付与されたバッジのリストを返す

    async def get_badges(self, user_id: int) -> list:
        """ユーザーの所持バッジ一覧を返す"""
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT badge_id FROM cesta_badges WHERE user_id = ?", (user_id,)
            ) as c:
                rows = await c.fetchall()
        return [r["badge_id"] for r in rows]

    async def has_badge(self, user_id: int, badge_id: str) -> bool:
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT 1 FROM cesta_badges WHERE user_id = ? AND badge_id = ?",
                (user_id, badge_id)
            ) as c:
                return bool(await c.fetchone())

    # ── /バッジ確認 ────────────────────────────────────────
    @app_commands.command(name="バッジ確認", description="自分のバッジと累計消費セスタを確認します")
    async def check_badges(self, interaction: discord.Interaction):
        user_id = interaction.user.id

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT total_spent FROM cesta_spent WHERE user_id = ?", (user_id,)
            ) as c:
                row = await c.fetchone()
            total = row["total_spent"] if row else 0

            async with db.execute(
                "SELECT badge_id, granted_at FROM cesta_badges WHERE user_id = ?", (user_id,)
            ) as c:
                badges = await c.fetchall()

            async with db.execute(
                "SELECT badge_id, threshold FROM cesta_badge_thresholds ORDER BY threshold ASC"
            ) as c:
                thresholds = await c.fetchall()

        owned = {b["badge_id"]: b["granted_at"] for b in badges}

        BADGE_EMOJI = {
            "入場券":    "🎟️",
            "道化師の証": "🃏",
            "座長の印":  "🎪",
        }

        embed = discord.Embed(
            title="🎪 サーカス バッジ",
            color=Color.CESTA
        )
        embed.add_field(
            name="💜 累計セスタ消費",
            value=f"**{total:,} セスタ**",
            inline=False
        )

        badge_text = ""
        for t in thresholds:
            bid   = t["badge_id"]
            thr   = t["threshold"]
            emoji = BADGE_EMOJI.get(bid, "🏅")
            if bid in owned:
                badge_text += f"{emoji} **{bid}** ✅ 取得済み\n"
            else:
                remaining = thr - total
                badge_text += f"{emoji} **{bid}** 🔒 あと **{remaining:,} セスタ**\n"

        embed.add_field(name="🏅 バッジ一覧", value=badge_text or "なし", inline=False)
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── 管理者: バッジ閾値変更 ──────────────────────────────
    @app_commands.command(name="バッジ閾値設定", description="【管理者】バッジ取得に必要な累計消費セスタを変更します")
    @app_commands.describe(
        badge="対象バッジ",
        threshold="必要累計消費セスタ"
    )
    @app_commands.choices(badge=[
        app_commands.Choice(name="🎟️ 入場券",    value="入場券"),
        app_commands.Choice(name="🃏 道化師の証", value="道化師の証"),
        app_commands.Choice(name="🎪 座長の印",  value="座長の印"),
    ])
    @has_permission("SUPREME_GOD")
    async def set_badge_threshold(
        self, interaction: discord.Interaction, badge: str, threshold: int
    ):
        if threshold <= 0:
            return await interaction.response.send_message(
                "❌ 1以上を指定してください。", ephemeral=True
            )
        async with self.bot.get_db() as db:
            await db.execute(
                "INSERT OR REPLACE INTO cesta_badge_thresholds (badge_id, threshold) VALUES (?, ?)",
                (badge, threshold)
            )
            await db.commit()
        await interaction.response.send_message(
            f"✅ **{badge}** の取得条件を **{threshold:,} セスタ消費** に変更しました。",
            ephemeral=True
        )

    # ── 管理者: バッジ手動付与 ──────────────────────────────
    @app_commands.command(name="バッジ付与", description="【管理者】指定ユーザーにバッジを手動付与します")
    @app_commands.choices(badge=[
        app_commands.Choice(name="🎟️ 入場券",    value="入場券"),
        app_commands.Choice(name="🃏 道化師の証", value="道化師の証"),
        app_commands.Choice(name="🎪 座長の印",  value="座長の印"),
    ])
    @has_permission("SUPREME_GOD")
    async def grant_badge_cmd(
        self, interaction: discord.Interaction,
        user: discord.Member, badge: str
    ):
        async with self.bot.get_db() as db:
            await db.execute(
                "INSERT OR IGNORE INTO cesta_badges (user_id, badge_id, granted_at) VALUES (?, ?, ?)",
                (user.id, badge, datetime.datetime.now().isoformat())
            )
            await db.commit()
        await interaction.response.send_message(
            f"✅ {user.mention} に **{badge}** を付与しました。", ephemeral=True
        )

# ================================================================
#   Cog: CestaShop
# ================================================================

BADGE_EMOJI = {
    "入場券":    "🎟️",
    "道化師の証": "🃏",
    "座長の印":  "🎪",
}
BADGE_ORDER = ["入場券", "道化師の証", "座長の印"]

class CestaShop(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    def _cesta(self) -> CestaSystem:
        return self.bot.get_cog("CestaSystem")

    # ── /セスタショップ ────────────────────────────────────
    @app_commands.command(name="セスタショップ", description="サーカスのセスタショップを開きます")
    async def cesta_shop(self, interaction: discord.Interaction):
        user_id   = interaction.user.id
        cesta_cog = self._cesta()

        bal    = await cesta_cog.get_balance(user_id)
        badges = await cesta_cog.get_badges(user_id)
        total  = await self._get_total_spent(user_id)

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM cesta_shop_items ORDER BY required_badge ASC, price ASC"
            ) as c:
                items = await c.fetchall()
            async with db.execute(
                "SELECT badge_id, threshold FROM cesta_badge_thresholds ORDER BY threshold ASC"
            ) as c:
                thresholds = await c.fetchall()

        embed = discord.Embed(
            title="🎪 サーカス セスタショップ",
            description=(
                f"💜 残高: **{bal:,} セスタ**\n"
                f"📊 累計消費: **{total:,} セスタ**\n\n"
                f"バッジを獲得すると新しい商品が解放されます！"
            ),
            color=Color.CESTA
        )

        # バッジ購入セクション
        badge_text = ""
        for t in thresholds:
            bid   = t["badge_id"]
            thr   = t["threshold"]
            emoji = BADGE_EMOJI.get(bid, "🏅")
            if bid in badges:
                badge_text += f"{emoji} **{bid}** ✅\n"
            else:
                rem = max(0, thr - total)
                badge_text += f"{emoji} **{bid}** 🔒 あと{rem:,}セスタ消費\n"
        embed.add_field(name="🏅 バッジ", value=badge_text or "なし", inline=False)

        # 商品セクション（バッジ階層ごとに分ける）
        if items:
            sections = {}
            for item in items:
                rb = item["required_badge"] or "なし"
                if rb not in sections:
                    sections[rb] = []
                sections[rb].append(item)

            for section_badge, section_items in sections.items():
                emoji    = BADGE_EMOJI.get(section_badge, "🛒")
                unlocked = section_badge == "なし" or section_badge in badges
                title    = f"{emoji} {section_badge}限定" if section_badge != "なし" else "🛒 一般商品"
                if not unlocked:
                    title += " 🔒"

                lines = []
                for item in section_items:
                    lock  = "" if unlocked else "~~"
                    itype = {"role": "ロール", "ticket": "商品券"}.get(item["item_type"], item["item_type"])
                    dur   = f"（{item['duration_days']}日間）" if item["duration_days"] > 0 else "（永続）" if item["item_type"] == "role" else ""
                    lines.append(
                        f"{lock}**{item['name']}** {dur}\n"
                        f"　{item['description']}\n"
                        f"　💜 {item['price']:,} セスタ　｜ {itype}{lock}"
                    )
                embed.add_field(
                    name=title,
                    value="\n".join(lines) if lines else "商品なし",
                    inline=False
                )
        else:
            embed.add_field(name="🛒 商品", value="現在商品はありません", inline=False)

        embed.set_footer(text="購入は /セスタショップ購入 から")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── /セスタショップ購入 ────────────────────────────────
    @app_commands.command(name="セスタショップ購入", description="セスタショップで商品を購入します")
    @app_commands.describe(item_id="購入する商品ID（/セスタショップ で確認）")
    async def cesta_shop_buy(self, interaction: discord.Interaction, item_id: str):
        user_id   = interaction.user.id
        cesta_cog = self._cesta()

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM cesta_shop_items WHERE item_id = ?", (item_id,)
            ) as c:
                item = await c.fetchone()

        if not item:
            return await interaction.response.send_message(
                "❌ 商品が見つかりません。IDを確認してください。", ephemeral=True
            )

        # バッジチェック
        required = item["required_badge"]
        if required:
            has = await cesta_cog.has_badge(user_id, required)
            if not has:
                emoji = BADGE_EMOJI.get(required, "🏅")
                return await interaction.response.send_message(
                    f"🔒 この商品は **{emoji}{required}** が必要です。", ephemeral=True
                )

        # 残高チェック
        bal = await cesta_cog.get_balance(user_id)
        if bal < item["price"]:
            return await interaction.response.send_message(
                f"❌ セスタが不足しています。\n"
                f"必要: **{item['price']:,}** / 所持: **{bal:,}**",
                ephemeral=True
            )

        await interaction.response.defer(ephemeral=True)
        now = datetime.datetime.now()

        async with self.bot.get_db() as db:
            # セスタ引き落とし＋消費記録
            ok = await cesta_cog.sub_balance(db, user_id, item["price"])
            if not ok:
                return await interaction.followup.send(
                    "❌ 残高が不足しています。", ephemeral=True
                )
            newly = await cesta_cog.record_spend(db, user_id, item["price"])

            try:
                if item["item_type"] == "role":
                    # ロール付与
                    if item["role_id"]:
                        role = interaction.guild.get_role(int(item["role_id"]))
                        if role:
                            await interaction.user.add_roles(role, reason=f"セスタショップ購入: {item['name']}")

                    # 期限管理
                    if item["duration_days"] > 0:
                        expiry = (now + datetime.timedelta(days=item["duration_days"])).isoformat()
                        await db.execute("""
                            INSERT INTO cesta_shop_subs (user_id, item_id, expiry)
                            VALUES (?, ?, ?)
                            ON CONFLICT(user_id, item_id) DO UPDATE SET expiry = excluded.expiry
                        """, (user_id, item_id, expiry))

                elif item["item_type"] == "ticket":
                    # 商品券をインベントリに追加
                    await db.execute("""
                        INSERT INTO cesta_tickets (user_id, item_id, item_name, purchased_at)
                        VALUES (?, ?, ?, ?)
                    """, (user_id, item_id, item["name"], now.isoformat()))

                await db.commit()

            except Exception as e:
                # ロール付与等に失敗したらロールバックしてセスタを返す
                await db.rollback()
                logger.error(f"CestaShop purchase error (user={user_id}, item={item_id}): {e}")
                return await interaction.followup.send(
                    "❌ 購入処理中にエラーが発生しました。セスタは消費されていません。\n"
                    "時間をおいて再度お試しください。",
                    ephemeral=True
                )
                
        new_bal = await cesta_cog.get_balance(user_id)

        embed = discord.Embed(
            title="✅ 購入完了！",
            color=Color.CESTA
        )
        itype = {"role": "ロール", "ticket": "商品券"}.get(item["item_type"], item["item_type"])
        dur   = f"{item['duration_days']}日間" if item["duration_days"] > 0 else "永続" if item["item_type"] == "role" else ""
        embed.add_field(
            name=item["name"],
            value=(
                f"{item['description']}\n"
                f"種別: {itype} {dur}\n"
                f"-{item['price']:,} セスタ"
            ),
            inline=False
        )
        embed.add_field(name="残高", value=f"{new_bal:,} セスタ", inline=True)

        # バッジ取得通知
        if newly:
            badge_notif = "\n".join(
                f"{BADGE_EMOJI.get(b, '🏅')} **{b}** を獲得しました！"
                for b in newly
            )
            embed.add_field(name="🎉 バッジ取得！", value=badge_notif, inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /セスタチケット確認 ────────────────────────────────
    @app_commands.command(name="セスタチケット確認", description="所持している商品券を確認します")
    async def cesta_tickets(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        async with self.bot.get_db() as db:
            async with db.execute("""
                SELECT * FROM cesta_tickets
                WHERE user_id = ? AND used_at IS NULL
                ORDER BY purchased_at DESC
            """, (user_id,)) as c:
                tickets = await c.fetchall()

        if not tickets:
            return await interaction.response.send_message(
                "🎟️ 未使用の商品券はありません。", ephemeral=True
            )

        embed = discord.Embed(title="🎟️ 所持商品券", color=Color.CESTA)
        for t in tickets:
            embed.add_field(
                name=f"#{t['id']} {t['item_name']}",
                value=f"購入日: {t['purchased_at'][:10]}",
                inline=False
            )
        embed.set_footer(text="商品券の使用は管理者に連絡してください")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── ユーティリティ ────────────────────────────────────
    async def _get_total_spent(self, user_id: int) -> int:
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT total_spent FROM cesta_spent WHERE user_id = ?", (user_id,)
            ) as c:
                row = await c.fetchone()
        return row["total_spent"] if row else 0

# ── /セスタショップ_商品登録 ───────────────────────────
    @app_commands.command(name="セスタショップ_商品登録", description="【管理者】セスタショップに商品を登録します")
    @app_commands.describe(
        item_id="商品ID（英数字推奨、例: joker_role）",
        name="商品名",
        description="商品説明",
        price="価格（セスタ）",
        item_type="商品種別",
        required_badge="必要バッジ（不要なら空欄）",
        role="付与するロール（ロール商品の場合）",
        duration_days="ロールの有効期限（日数、0で永続）",
    )
    @app_commands.choices(
        item_type=[
            app_commands.Choice(name="ロール", value="role"),
            app_commands.Choice(name="商品券", value="ticket"),
        ],
        required_badge=[
            app_commands.Choice(name="なし",       value=""),
            app_commands.Choice(name="🎟️ 入場券",    value="入場券"),
            app_commands.Choice(name="🃏 道化師の証", value="道化師の証"),
            app_commands.Choice(name="🎪 座長の印",  value="座長の印"),
        ]
    )
    @has_permission("SUPREME_GOD")
    async def shop_add_item(
        self,
        interaction: discord.Interaction,
        item_id:        str,
        name:           str,
        description:    str,
        price:          int,
        item_type:      str,
        required_badge: str = "",
        role:           Optional[discord.Role] = None,
        duration_days:  int = 0,
    ):
        if price <= 0:
            return await interaction.response.send_message(
                "❌ 価格は1以上を指定してください。", ephemeral=True
            )
        if item_type == "role" and not role:
            return await interaction.response.send_message(
                "❌ ロール商品にはロールを指定してください。", ephemeral=True
            )
        role_id = role.id if role else None
        async with self.bot.get_db() as db:
            await db.execute("""
                INSERT OR REPLACE INTO cesta_shop_items
                    (item_id, name, description, price, item_type, required_badge, role_id, duration_days)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item_id, name, description, price,
                item_type, required_badge or None,
                role_id, duration_days
            ))
            await db.commit()
        embed = discord.Embed(title="✅ 商品登録完了", color=Color.CESTA)
        itype = "ロール" if item_type == "role" else "商品券"
        dur   = f"{duration_days}日間" if duration_days > 0 else "永続" if item_type == "role" else "-"
        embed.add_field(name="商品ID",   value=item_id,           inline=True)
        embed.add_field(name="商品名",   value=name,              inline=True)
        embed.add_field(name="価格",     value=f"{price:,} セスタ", inline=True)
        embed.add_field(name="種別",     value=f"{itype} / {dur}", inline=True)
        embed.add_field(
            name="必要バッジ",
            value=f"{BADGE_EMOJI.get(required_badge, '')} {required_badge}" if required_badge else "なし",
            inline=True
        )
        if role:
            embed.add_field(name="付与ロール", value=role.mention, inline=True)
        embed.add_field(name="説明", value=description, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── /セスタショップ_商品削除 ───────────────────────────
    @app_commands.command(name="セスタショップ_商品削除", description="【管理者】セスタショップから商品を削除します")
    @app_commands.describe(item_id="削除する商品ID")
    @has_permission("SUPREME_GOD")
    async def shop_remove_item(self, interaction: discord.Interaction, item_id: str):
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT name FROM cesta_shop_items WHERE item_id = ?", (item_id,)
            ) as c:
                row = await c.fetchone()
        if not row:
            return await interaction.response.send_message(
                "❌ 商品が見つかりません。", ephemeral=True
            )
        async with self.bot.get_db() as db:
            await db.execute("DELETE FROM cesta_shop_items WHERE item_id = ?", (item_id,))
            await db.commit()
        await interaction.response.send_message(
            f"🗑️ **{row['name']}**（{item_id}）を削除しました。", ephemeral=True
        )

    # ── /セスタショップ_商品一覧 ───────────────────────────
    @app_commands.command(name="セスタショップ_商品一覧", description="【管理者】登録済み商品の一覧を確認します")
    @has_permission("ADMIN")
    async def shop_list_items(self, interaction: discord.Interaction):
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM cesta_shop_items ORDER BY required_badge ASC, price ASC"
            ) as c:
                items = await c.fetchall()
        if not items:
            return await interaction.response.send_message(
                "📝 登録されている商品はありません。", ephemeral=True
            )
        embed = discord.Embed(title="📦 セスタショップ 商品一覧", color=Color.CESTA)
        for item in items:
            itype = "ロール" if item["item_type"] == "role" else "商品券"
            dur   = f"{item['duration_days']}日" if item["duration_days"] > 0 else "永続" if item["item_type"] == "role" else "-"
            rb    = f"{BADGE_EMOJI.get(item['required_badge'], '')} {item['required_badge']}" if item["required_badge"] else "なし"
            embed.add_field(
                name=f"`{item['item_id']}` {item['name']}",
                value=(
                    f"💜 {item['price']:,} セスタ　｜ {itype} / {dur}\n"
                    f"必要バッジ: {rb}\n"
                    f"{item['description']}"
                ),
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── /セスタショップ_ロール期限確認 ─────────────────────
    @app_commands.command(name="セスタショップ_ロール期限確認", description="【管理者】期限付きロールの有効期限一覧を確認します")
    @has_permission("ADMIN")
    async def shop_check_subs(self, interaction: discord.Interaction):
        async with self.bot.get_db() as db:
            async with db.execute("""
                SELECT s.user_id, s.item_id, s.expiry, i.name, i.role_id
                FROM cesta_shop_subs s
                JOIN cesta_shop_items i ON s.item_id = i.item_id
                ORDER BY s.expiry ASC
            """) as c:
                subs = await c.fetchall()
        if not subs:
            return await interaction.response.send_message(
                "📝 期限付きロールの購入者はいません。", ephemeral=True
            )
        now   = datetime.datetime.now()
        embed = discord.Embed(title="⏰ 期限付きロール一覧", color=Color.CESTA)
        for s in subs:
            expiry  = datetime.datetime.fromisoformat(s["expiry"])
            expired = expiry < now
            user    = interaction.guild.get_member(s["user_id"])
            uname   = user.display_name if user else f"ID:{s['user_id']}"
            status  = "❌ 期限切れ" if expired else f"✅ {expiry.strftime('%Y/%m/%d')}"
            embed.add_field(name=f"{uname} / {s['name']}", value=status, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ── /セスタショップ_期限切れ処理 ──────────────────────
    @app_commands.command(name="セスタショップ_期限切れ処理", description="【管理者】期限切れロールを一括で剥奪します")
    @has_permission("SUPREME_GOD")
    async def shop_expire_roles(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        now = datetime.datetime.now()
        async with self.bot.get_db() as db:
            async with db.execute("""
                SELECT s.user_id, s.item_id, i.name, i.role_id
                FROM cesta_shop_subs s
                JOIN cesta_shop_items i ON s.item_id = i.item_id
                WHERE s.expiry < ?
            """, (now.isoformat(),)) as c:
                expired = await c.fetchall()

        if not expired:
            return await interaction.followup.send("✅ 期限切れのロールはありません。", ephemeral=True)

        removed = []
        errors  = []
        async with self.bot.get_db() as db:
            for e in expired:
                user = interaction.guild.get_member(e["user_id"])
                if user and e["role_id"]:
                    role = interaction.guild.get_role(int(e["role_id"]))
                    if role:
                        try:
                            await user.remove_roles(role, reason="セスタショップ期限切れ")
                            removed.append(f"{user.display_name} / {e['name']}")
                        except Exception as ex:
                            errors.append(f"{e['user_id']}: {ex}")
                            continue

                await db.execute(
                    "DELETE FROM cesta_shop_subs WHERE user_id = ? AND item_id = ?",
                    (e["user_id"], e["item_id"])
                )
            await db.commit()

        lines = "\n".join(f"🗑️ {r}" for r in removed) or "なし"
        embed = discord.Embed(title="🗑️ 期限切れ処理完了", color=Color.CESTA)
        embed.add_field(name=f"剥奪({len(removed)}件)", value=lines, inline=False)
        if errors:
            embed.add_field(name="エラー", value="\n".join(errors), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

            
        lines = "\n".join(f"🗑️ {r}" for r in removed) or "なし"
        embed = discord.Embed(title="🗑️ 期限切れ処理完了", color=Color.CESTA)
        embed.add_field(name=f"剥奪({len(removed)}件)", value=lines, inline=False)
        if errors:
            embed.add_field(name="エラー", value="\n".join(errors), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /セスタショップ_チケット使用 ──────────────────────
    @app_commands.command(name="セスタショップ_チケット使用", description="【管理者】ユーザーの商品券を使用済みにします")
    @app_commands.describe(ticket_id="チケットID（/セスタチケット確認 で確認）", user="対象ユーザー")
    @has_permission("ADMIN")
    async def shop_use_ticket(
        self, interaction: discord.Interaction,
        user: discord.Member, ticket_id: int
    ):
        now = datetime.datetime.now()
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM cesta_tickets WHERE id = ? AND user_id = ? AND used_at IS NULL",
                (ticket_id, user.id)
            ) as c:
                ticket = await c.fetchone()
            if not ticket:
                return await interaction.response.send_message(
                    "❌ チケットが見つからないか、すでに使用済みです。", ephemeral=True
                )
            await db.execute(
                "UPDATE cesta_tickets SET used_at = ?, used_by = ? WHERE id = ?",
                (now.isoformat(), interaction.user.id, ticket_id)
            )
            await db.commit()
        await interaction.response.send_message(
            f"✅ {user.mention} の **{ticket['item_name']}**（#{ticket_id}）を使用済みにしました。",
            ephemeral=True
        )
                
# ================================================================
#   人間株式市場 (完全版: スター豪華演出 + 昇格システム)
# ================================================================

# ── 取引パネル (View) ──
class StockControlView(discord.ui.View):
    def __init__(self, cog, target_user: discord.Member):
        super().__init__(timeout=300)
        self.cog = cog
        self.target = target_user

    async def update_embed(self, interaction: discord.Interaction):
        # 1. DBから最新情報を取得
        star_role_id = None
        async with self.cog.bot.get_db() as db:
            # スターロールIDの確認
            async with db.execute("SELECT value FROM market_config WHERE key = 'star_role_id'") as c:
                row = await c.fetchone()
                if row: star_role_id = int(row['value'])

            # 発行株数の確認
            async with db.execute("SELECT total_shares FROM stock_issuers WHERE user_id = ?", (self.target.id,)) as c:
                row = await c.fetchone()
                if not row: return None 
                shares = row['total_shares']
            
            # 自分の保有状況の確認
            async with db.execute("SELECT amount, avg_cost FROM stock_holdings WHERE user_id = ? AND issuer_id = ?", (interaction.user.id, self.target.id)) as c:
                holding = await c.fetchone()
                my_amount = holding['amount'] if holding else 0
                my_avg = holding['avg_cost'] if holding else 0

        # 2. スター判定（ターゲットがスターロールを持っているか？）
        is_star = False
        if star_role_id:
            if any(r.id == star_role_id for r in self.target.roles):
                is_star = True

        current_price = self.cog.calculate_price(shares)
        
        # 3. 損益計算
        total_val = current_price * my_amount
        profit = total_val - (my_avg * my_amount)
        sign = "+" if profit >= 0 else ""

        # 4. デザインの分岐
        if is_star:
            color = 0xFFD700 # ゴールド
            title = f"👑 {self.target.display_name} 👑"
            desc = "✨ **STAR MEMBER** ✨\n現在ランキング上位のスター銘柄です。\n価格変動が激しい可能性があります。"
            thumbnail_url = self.target.display_avatar.url
        else:
            # 通常デザイン（利益が出てれば緑、損失なら赤）
            color = 0x00ff00 if profit >= 0 else 0xff0000
            title = f"📈 {self.target.display_name} の銘柄"
            desc = "ボタンで売買できます（手数料: 10%）"
            thumbnail_url = self.target.display_avatar.url
        
        embed = discord.Embed(title=title, description=desc, color=color)
        embed.set_thumbnail(url=thumbnail_url)
        
        # 5. フィールド設定
        # スターの場合は少しリッチな装飾文字を使う
        icon_price = "💎" if is_star else "💰"
        icon_stock = "🏰" if is_star else "🏢"

        embed.add_field(name=f"{icon_price} 現在株価", value=f"**{current_price:,} S**", inline=True)
        embed.add_field(name=f"{icon_stock} 発行数", value=f"{shares:,} 株", inline=True)
        
        # 空白フィールドで段落調整
        embed.add_field(name="\u200b", value="\u200b", inline=True) 

        # 保有情報の表示
        embed.add_field(name="──────────", value="**あなたの保有状況**", inline=False)
        embed.add_field(name="🎒 保有数", value=f"{my_amount:,} 株", inline=True)
        
        # 損益表示（スターで色が固定されても、損益は文字色で見やすくする）
        profit_str = f"{sign}{int(profit):,} S"
        if profit >= 0:
            val_str = f"```ansi\n\u001b[1;32m{profit_str}\u001b[0m```" # 緑
        else:
            val_str = f"```ansi\n\u001b[1;31m{profit_str}\u001b[0m```" # 赤
            
        embed.add_field(name="📊 評価損益", value=val_str, inline=True)
        
        if is_star:
            embed.set_footer(text="★ スター銘柄: 2週間ごとの審査で入れ替わります")
        
        return embed

    # ── ボタン処理 ──
    @discord.ui.button(label="買う(1)", style=discord.ButtonStyle.success, emoji="🛒", row=0)
    async def buy_one(self, interaction, button): await self._trade(interaction, "buy", 1)

    @discord.ui.button(label="買う(10)", style=discord.ButtonStyle.success, emoji="📦", row=0)
    async def buy_ten(self, interaction, button): await self._trade(interaction, "buy", 10)

    @discord.ui.button(label="売る(1)", style=discord.ButtonStyle.danger, emoji="💸", row=1)
    async def sell_one(self, interaction, button): await self._trade(interaction, "sell", 1)

    @discord.ui.button(label="全売却", style=discord.ButtonStyle.danger, emoji="💥", row=1)
    async def sell_all(self, interaction, button):
        async with self.cog.bot.get_db() as db:
            async with db.execute("SELECT amount FROM stock_holdings WHERE user_id = ? AND issuer_id = ?", (interaction.user.id, self.target.id)) as c:
                row = await c.fetchone()
                amount = row['amount'] if row else 0
        if amount > 0: await self._trade(interaction, "sell", amount)
        else: await interaction.response.send_message("株を持っていません。", ephemeral=True)

    @discord.ui.button(label="更新", style=discord.ButtonStyle.secondary, emoji="🔄", row=1)
    async def refresh(self, interaction, button):
        new_embed = await self.update_embed(interaction)
        if new_embed: await interaction.response.edit_message(embed=new_embed, view=self)

    async def _trade(self, interaction, type, amount):
        if type == "buy": msg, success = await self.cog.internal_buy(interaction.user, self.target, amount)
        else: msg, success = await self.cog.internal_sell(interaction.user, self.target, amount)
        
        if success:
            new_embed = await self.update_embed(interaction)
            await interaction.response.edit_message(embed=new_embed, view=self)
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


# ── 本体 (Cog) ──
class HumanStockMarket(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # ── 市場設定 ──
        self.base_price = 100       # 最低価格
        self.slope = 20             # 価格感応度（1株ごとの値上がり幅）
        self.trading_fee = 0.10     # 手数料10%
        self.issuer_fee = 0.05      # 発行者への還元5%
        
        self.promotion_cycle_task.start() # 昇格審査タスクを開始

    def cog_unload(self):
        self.promotion_cycle_task.cancel()

    # 価格計算式（ボンディングカーブ）
    def calculate_price(self, shares):
        return self.base_price + (shares * self.slope)

    async def init_market_db(self):
        async with self.bot.get_db() as db:
            await db.execute("CREATE TABLE IF NOT EXISTS stock_issuers (user_id INTEGER PRIMARY KEY, total_shares INTEGER DEFAULT 0, is_listed INTEGER DEFAULT 1)")
            await db.execute("CREATE TABLE IF NOT EXISTS stock_holdings (user_id INTEGER, issuer_id INTEGER, amount INTEGER, avg_cost REAL, PRIMARY KEY (user_id, issuer_id))")
            await db.execute("CREATE TABLE IF NOT EXISTS market_config (key TEXT PRIMARY KEY, value TEXT)")
            await db.commit()

    # ── 昇格・入れ替えシステム (2週間ごとのランキング集計) ──
    @tasks.loop(hours=1) # 1時間ごとにチェック
    async def promotion_cycle_task(self):
        await self.bot.wait_until_ready()
        now = datetime.datetime.now()
        
        async with self.bot.get_db() as db:
            # 次回の審査日時を取得
            async with db.execute("SELECT value FROM market_config WHERE key = 'next_promotion_date'") as c:
                row = await c.fetchone()
                if row:
                    next_date = datetime.datetime.fromisoformat(row['value'])
                else:
                    # 設定がない場合は現在時刻から2週間後をセット
                    next_date = now + datetime.timedelta(weeks=2)
                    await db.execute("INSERT OR REPLACE INTO market_config (key, value) VALUES ('next_promotion_date', ?)", (next_date.isoformat(),))
                    await db.commit()
                    return # 初回セット時はスキップ

        # 審査時刻を過ぎていたら実行
        if now >= next_date:
            await self.execute_promotion(now)

    async def execute_promotion(self, now):
        guild = self.bot.guilds[0] # メインサーバーを想定
        cast_role_id = None
        star_role_id = None
        log_ch_id = None

        # 設定読み込み
        async with self.bot.get_db() as db:
            async with db.execute("SELECT key, value FROM market_config") as c:
                async for row in c:
                    if row['key'] == 'cast_role_id': cast_role_id = int(row['value'])
                    elif row['key'] == 'star_role_id': star_role_id = int(row['value'])
                    elif row['key'] == 'promotion_log_id': log_ch_id = int(row['value'])
            
            # ランキング集計（株価が高い順 = 発行数が多い順）
            async with db.execute("SELECT user_id, total_shares FROM stock_issuers WHERE is_listed=1 ORDER BY total_shares DESC") as c:
                rankings = await c.fetchall()

        if not cast_role_id or not star_role_id:
            logger.error("Roles for Stock Market promotion are not set.")
            return

        cast_role = guild.get_role(cast_role_id)
        star_role = guild.get_role(star_role_id)
        if not cast_role or not star_role: return

        # 上位4名を特定
        top_4_ids = []
        promoted_members = []
        demoted_members = []

        # ランキング上位からループして、キャストロールを持っている人を探す
        for row in rankings:
            if len(top_4_ids) >= 4: break
            
            member = guild.get_member(row['user_id'])
            if member and cast_role in member.roles: # キャストロール所持者のみ対象
                top_4_ids.append(member.id)

        # 1. スターロールの付与と剥奪処理
        # 現在スターロールを持っている全員をチェック
        for member in star_role.members:
            if member.id not in top_4_ids:
                try:
                    await member.remove_roles(star_role, reason="株価ランキング圏外による降格")
                    demoted_members.append(member.display_name)
                except: pass
        
        # 新トップ4にスターロール付与
        for uid in top_4_ids:
            member = guild.get_member(uid)
            if member:
                if star_role not in member.roles:
                    try:
                        await member.add_roles(star_role, reason="株価ランキングTop4入り")
                        promoted_members.append(member.display_name)
                    except: pass

        # 次回の日程を更新 (2週間後)
        next_due = now + datetime.timedelta(weeks=2)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO market_config (key, value) VALUES ('next_promotion_date', ?)", (next_due.isoformat(),))
            await db.commit()

        # ログ・通知送信
        if log_ch_id:
            channel = self.bot.get_channel(log_ch_id)
            if channel:
                embed = discord.Embed(title="👑 キャスト選抜総選挙 結果発表", description="株価ランキングによるスター入れ替えが行われました。", color=Color.STELL)
                
                top_text = ""
                for i, uid in enumerate(top_4_ids):
                    m = guild.get_member(uid)
                    name = m.display_name if m else "Unknown"
                    share_val = 0
                    # 株価取得用
                    for r in rankings:
                        if r['user_id'] == uid:
                            share_val = self.calculate_price(r['total_shares'])
                            break
                    top_text += f"**{i+1}位**: {name} (株価: {share_val:,} S)\n"
                
                if not top_text: top_text = "該当者なし"

                embed.add_field(name="🏆 新スターメンバー (Top 4)", value=top_text, inline=False)
                
                if promoted_members:
                    embed.add_field(name="⬆️ 新規昇格", value=", ".join(promoted_members), inline=True)
                if demoted_members:
                    embed.add_field(name="⬇️ 降格", value=", ".join(demoted_members), inline=True)
                
                embed.set_footer(text=f"次回審査: {next_due.strftime('%Y/%m/%d %H:%M')}")
                await channel.send(embed=embed)


    # ── 内部処理: 購入 ──
    async def internal_buy(self, buyer, target, amount):
        if buyer.id == target.id: return ("❌ 自己売買は禁止です。", False)
        
        async with self.bot.get_db() as db:
            async with db.execute("SELECT total_shares FROM stock_issuers WHERE user_id = ?", (target.id,)) as c:
                row = await c.fetchone()
                if not row: return ("❌ 上場していません。", False)
                shares = row['total_shares']

            # 価格計算
            unit_price = self.calculate_price(shares)
            
            # 購入処理
            subtotal = unit_price * amount
            fee = int(subtotal * self.trading_fee)
            bonus = int(subtotal * self.issuer_fee)
            total = subtotal + fee + bonus

            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (buyer.id,)) as c:
                bal = await c.fetchone()
                if not bal or bal['balance'] < total: return (f"❌ 資金不足 (必要: {total:,} S)", False)

            try:
                # 資産移動
                await db.execute("UPDATE accounts SET balance = balance - ? WHERE user_id = ?", (total, buyer.id))
                await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (bonus, target.id)) # 発行者へ還元
                
                # 保有データ更新
                async with db.execute("SELECT amount, avg_cost FROM stock_holdings WHERE user_id = ? AND issuer_id = ?", (buyer.id, target.id)) as c:
                    h = await c.fetchone()
                
                if h:
                    new_n = h['amount'] + amount
                    # 平均取得単価の更新
                    new_avg = ((h['amount'] * h['avg_cost']) + subtotal) / new_n
                    await db.execute("UPDATE stock_holdings SET amount = ?, avg_cost = ? WHERE user_id = ? AND issuer_id = ?", (new_n, new_avg, buyer.id, target.id))
                else:
                    await db.execute("INSERT INTO stock_holdings (user_id, issuer_id, amount, avg_cost) VALUES (?, ?, ?, ?)", (buyer.id, target.id, amount, unit_price))
                
                # 発行数増加（これにより次の人の購入価格が上がる）
                await db.execute("UPDATE stock_issuers SET total_shares = total_shares + ? WHERE user_id = ?", (amount, target.id))
                
                month = datetime.datetime.now().strftime("%Y-%m")
                await db.execute("INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag) VALUES (?, ?, ?, 'STOCK_BUY', ?, ?)",
                                 (buyer.id, 0, total, f"株購入: {target.display_name}", month))
                await db.commit()
                return (f"✅ 購入成功: {target.display_name} x{amount}株 (単価: {unit_price:,} S)", True)
            except Exception as e:
                await db.rollback()
                return (f"エラー: {e}", False)

    # ── 内部処理: 売却 ──
    async def internal_sell(self, seller, target, amount):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT total_shares FROM stock_issuers WHERE user_id = ?", (target.id,)) as c:
                row = await c.fetchone()
                if not row: return ("❌ 上場していません。", False)
                shares = row['total_shares']

            async with db.execute("SELECT amount, avg_cost FROM stock_holdings WHERE user_id = ? AND issuer_id = ?", (seller.id, target.id)) as c:
                h = await c.fetchone()
                if not h or h['amount'] < amount: return ("❌ 保有数不足", False)

            # 現在価格で売却（売るときは少し安くなる＝スプレッド要素として、base_price計算を現在発行数ベースで行う）
            unit_price = self.calculate_price(shares)
            revenue = unit_price * amount
            
            try:
                new_n = h['amount'] - amount
                if new_n == 0: await db.execute("DELETE FROM stock_holdings WHERE user_id = ? AND issuer_id = ?", (seller.id, target.id))
                else: await db.execute("UPDATE stock_holdings SET amount = ? WHERE user_id = ? AND issuer_id = ?", (new_n, seller.id, target.id))
                
                await db.execute("UPDATE accounts SET balance = balance + ? WHERE user_id = ?", (revenue, seller.id))
                # 発行数を減らす（価格が下がる）
                await db.execute("UPDATE stock_issuers SET total_shares = total_shares - ? WHERE user_id = ?", (amount, target.id))
                
                month = datetime.datetime.now().strftime("%Y-%m")
                await db.execute("INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag) VALUES (0, ?, ?, 'STOCK_SELL', ?, ?)",
                                 (seller.id, revenue, f"株売却: {target.display_name}", month))
                await db.commit()
                return (f"📉 売却成功: {revenue:,} S 受取", True)
            except Exception as e:
                await db.rollback()
                return (f"エラー: {e}", False)

    # ── コマンド類 ──

    @app_commands.command(name="株_キャスト設定", description="【管理者】上場可能な『キャスト』ロールを設定します")
    @has_permission("ADMIN")
    async def config_cast_role(self, interaction: discord.Interaction, role: discord.Role):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO market_config (key, value) VALUES ('cast_role_id', ?)", (str(role.id),))
            await db.commit()
        await interaction.followup.send(f"✅ 上場可能ロールを {role.mention} に設定しました。", ephemeral=True)

    @app_commands.command(name="株_スター設定", description="【管理者】ランキング上位に付与する『スター』ロールを設定します")
    @has_permission("ADMIN")
    async def config_star_role(self, interaction: discord.Interaction, role: discord.Role):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO market_config (key, value) VALUES ('star_role_id', ?)", (str(role.id),))
            await db.commit()
        await interaction.followup.send(f"✅ 上位報酬ロールを {role.mention} に設定しました。", ephemeral=True)

    @app_commands.command(name="株_結果ログ設定", description="【管理者】昇格・降格の結果を発表するチャンネルを設定します")
    @has_permission("ADMIN")
    async def config_promo_log(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO market_config (key, value) VALUES ('promotion_log_id', ?)", (str(channel.id),))
            await db.commit()
        await interaction.followup.send(f"✅ 結果発表先を {channel.mention} に設定しました。", ephemeral=True)

    @app_commands.command(name="株_上場", description="自分の株を上場します（キャスト限定）")
    async def ipo(self, interaction):
        await self.init_market_db()
        user = interaction.user

        # ロールチェック
        cast_role_id = None
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM market_config WHERE key = 'cast_role_id'") as c:
                row = await c.fetchone()
                if row: cast_role_id = int(row['value'])
        
        if not cast_role_id:
            return await interaction.response.send_message("❌ システムエラー: キャストロールが未設定です。管理者に連絡してください。", ephemeral=True)

        has_cast_role = any(r.id == cast_role_id for r in user.roles)
        if not has_cast_role:
             return await interaction.response.send_message("❌ 上場できるのは『キャスト』のみです。", ephemeral=True)

        async with self.bot.get_db() as db:
            try:
                await db.execute("INSERT INTO stock_issuers (user_id, total_shares) VALUES (?, 0)", (user.id,))
                await db.commit()
                await interaction.response.send_message(f"🎉 {user.mention} が株式市場に上場しました！\n誰でもこの株を売買して利益を狙えます。")
            except:
                await interaction.response.send_message("既に上場済みです。", ephemeral=True)

    @app_commands.command(name="株_取引パネル", description="株の売買パネルを開きます")
    async def open_panel(self, interaction: discord.Interaction, target: discord.Member):
        await self.init_market_db()
        view = StockControlView(self, target)
        embed = await view.update_embed(interaction)
        if embed: await interaction.response.send_message(embed=embed, view=view)
        else: await interaction.response.send_message("その人は上場していません。", ephemeral=True)

    @app_commands.command(name="株_ランキング", description="現在の株価ランキングと次回の審査日を表示します")
    async def ranking(self, interaction: discord.Interaction):
        await self.init_market_db()
        await interaction.response.defer()
        
        next_date_str = "未定"
        async with self.bot.get_db() as db:
            async with db.execute("SELECT user_id, total_shares FROM stock_issuers WHERE is_listed=1") as c: rows = await c.fetchall()
            async with db.execute("SELECT value FROM market_config WHERE key = 'next_promotion_date'") as c:
                row = await c.fetchone()
                if row:
                    dt = datetime.datetime.fromisoformat(row['value'])
                    next_date_str = dt.strftime("%m/%d %H:%M")

        data = []
        for r in rows:
            p = self.calculate_price(r['total_shares'])
            m = interaction.guild.get_member(r['user_id'])
            # 退室したメンバーなどは除外
            if not m: continue
            
            name = m.display_name
            data.append((name, p, r['total_shares']))
        
        # 株価順（=発行数順）にソート
        data.sort(key=lambda x: x[1], reverse=True)
        
        desc = f"📅 **次回審査: {next_date_str}**\n上位4名が『スター』に昇格します。\n\n"
        
        for i, d in enumerate(data[:10]):
            rank_icon = "👑" if i < 4 else f"{i+1}."
            bold = "**" if i < 4 else ""
            line = f"{rank_icon} {bold}{d[0]}{bold}: 株価 {d[1]:,} S (流通: {d[2]}株)\n"
            desc += line
            
        if len(data) > 10: desc += f"\n...他 {len(data)-10} 名"

        embed = discord.Embed(title="📊 キャスト株価ランキング", description=desc, color=Color.STELL)
        embed.set_footer(text="株を買うと価格が上がり、売ると下がります。推しをスターに押し上げよう！")
        await interaction.followup.send(embed=embed)


class ServerStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        if not self.daily_log_task.is_running():
            self.daily_log_task.start()

    def cog_unload(self):
        self.daily_log_task.cancel()

    # ── ジニ係数計算 ──────────────────────────────────────
    def _calc_gini(self, balances: list) -> float:
        if not balances or sum(balances) == 0:
            return 0.0
        s = sorted(balances)
        n = len(s)
        total = sum(s)
        return (2 * sum((i + 1) * v for i, v in enumerate(s)) / (n * total)) - (n + 1) / n

# ── 市民の残高リストを取得 ─────────────────────────────
    async def _get_citizen_balances(self) -> list[int]:
        guild = self.bot.guilds[0]
        await guild.chunk()
        member_map = {m.id: m for m in guild.members}

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT value FROM server_config WHERE key = 'citizen_role_id'"
            ) as c:
                row = await c.fetchone()
            citizen_role_id = int(row["value"]) if row else None

            god_role_ids = {
                r_id for r_id, level in self.bot.config.admin_roles.items()
                if level == "SUPREME_GOD"
            }

            async with db.execute("SELECT user_id, balance FROM accounts WHERE user_id != 0") as c:
                all_accounts = await c.fetchall()

        balances = []
        for row in all_accounts:
            uid, bal = row["user_id"], row["balance"]
            member = member_map.get(uid)
            if not member or member.bot:
                continue
            if any(r.id in god_role_ids for r in member.roles):
                continue
            if citizen_role_id and not any(r.id == citizen_role_id for r in member.roles):
                continue
            balances.append(bal)
        return balances
        
    # ── 24時間タスク ──────────────────────────────────────
    @tasks.loop(hours=24)
    async def daily_log_task(self):
        try:
            balances = await self._get_citizen_balances()
            total    = sum(balances)
            gini     = self._calc_gini(balances)
            today    = datetime.datetime.now().strftime("%Y-%m-%d")

            # セスタ総量
            async with self.bot.get_db() as db:
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date          TEXT PRIMARY KEY,
                        total_stell   INTEGER DEFAULT 0,
                        total_cesta   INTEGER DEFAULT 0,
                        gini          REAL    DEFAULT 0
                    )
                """)
                async with db.execute("SELECT SUM(balance) FROM cesta_wallets") as c:
                    row = await c.fetchone()
                total_cesta = row[0] or 0

                await db.execute("""
                    INSERT OR REPLACE INTO daily_stats (date, total_stell, total_cesta, gini)
                    VALUES (?, ?, ?, ?)
                """, (today, total, total_cesta, gini))
                await db.commit()
        except Exception as e:
            logger.error(f"Daily Log Error: {e}")

    @daily_log_task.before_loop
    async def before_daily_log(self):
        await self.bot.wait_until_ready()
        
    # ── /経済レポート ──────────────────────────────────────
    @app_commands.command(name="経済レポート", description="サーバー経済の現状レポートを表示します")
    @has_permission("ADMIN")
    async def economy_report(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            # 現在の市民残高
            balances = await self._get_citizen_balances()
            balances.sort()
            count       = len(balances)
            total_stell = sum(balances)
            avg         = total_stell // count if count else 0
            median      = balances[count // 2] if balances else 0
            gini        = self._calc_gini(balances)

            # セスタ総量
            async with self.bot.get_db() as db:
                async with db.execute("SELECT SUM(balance) FROM cesta_wallets") as c:
                    row = await c.fetchone()
                total_cesta = row[0] or 0

                # 7日前のデータ
                week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
                async with db.execute(
                    "SELECT total_stell, total_cesta, gini FROM daily_stats WHERE date <= ? ORDER BY date DESC LIMIT 1",
                    (week_ago,)
                ) as c:
                    old = await c.fetchone()

                # 24時間の資金フロー（自然 vs 運営操作）
                cutoff_24h = datetime.datetime.now() - datetime.timedelta(days=1)
                natural_mint = natural_burn = op_add = op_remove = 0
                op_add_count = op_remove_count = 0

                async with db.execute(
                    "SELECT sender_id, receiver_id, amount, type FROM transactions WHERE created_at > ?",
                    (cutoff_24h,)
                ) as c:
                    async for row in c:
                        s_id, r_id, amt, t_type = row["sender_id"], row["receiver_id"], row["amount"], row["type"]
                        if t_type == "SYSTEM_ADD":
                            op_add += amt
                            op_add_count += 1
                        elif t_type == "SYSTEM_REMOVE":
                            op_remove += amt
                            op_remove_count += 1
                        elif s_id == 0:
                            natural_mint += amt
                        elif r_id == 0:
                            natural_burn += amt

            # ── ステータス判定 ──
            # インフレ・デフレ
            if old and old["total_stell"] > 0:
                stell_change_pct = (total_stell - old["total_stell"]) / old["total_stell"] * 100
            else:
                stell_change_pct = None

            if stell_change_pct is None:
                inflation_status = "- 比較データなし"
            elif stell_change_pct >= 5:
                inflation_status = "🔴 深刻なインフレ"
            elif stell_change_pct >= 2:
                inflation_status = "🟠 インフレ傾向"
            elif stell_change_pct >= -2:
                inflation_status = "🟢 安定"
            elif stell_change_pct >= -5:
                inflation_status = "🟡 デフレ傾向"
            else:
                inflation_status = "🔴 深刻なデフレ"

            # 格差
            old_gini = old["gini"] if old else None
            gini_diff = gini - old_gini if old_gini is not None else None

            if count == 0:
                gap_status = "- データなし"
            elif gini < 0.3:
                gap_status = "🟢 健全"
            elif gini < 0.4:
                gap_status = "🟡 格差あり"
            elif gini < 0.5 or (gini_diff is not None and gini_diff > 0.03):
                gap_status = "🟠 格差拡大中"
            else:
                gap_status = "🔴 深刻な格差"

            # ── 変化表示 ──
            def diff_str(new, old_val, unit="S"):
                if old_val is None or old_val == 0:
                    return ""
                diff = new - old_val
                pct  = diff / old_val * 100
                sign = "+" if diff >= 0 else ""
                return f"（先週比 {sign}{pct:.1f}%）"

            stell_diff  = diff_str(total_stell, old["total_stell"] if old else None)
            cesta_diff  = diff_str(total_cesta, old["total_cesta"] if old else None, "C")
            gini_str    = f"{gini:.3f}"
            if gini_diff is not None:
                arrow = "↑" if gini_diff > 0 else "↓" if gini_diff < 0 else "→"
                gini_str += f"（先週 {old_gini:.3f} {arrow}）"

            natural_net  = natural_mint - natural_burn
            natural_sign = "+" if natural_net >= 0 else ""

            # ── Embed構築 ──
            embed = discord.Embed(title="経済レポート", color=Color.DARK)
            embed.description = (
                f"{inflation_status}\n"
                f"{gap_status}\n"
            )

            embed.add_field(
                name="Stell",
                value=(
                    f"`{total_stell:,} S` {stell_diff}\n"
                    f"平均 {avg:,} S　中央値 {median:,} S\n"
                    f"市民 {count} 人"
                ),
                inline=False
            )
            embed.add_field(
                name="セスタ",
                value=f"`{total_cesta:,} C` {cesta_diff}",
                inline=False
            )
            embed.add_field(
                name="ジニ係数",
                value=gini_str,
                inline=False
            )
            embed.add_field(
                name="24時間の自然な動き",
                value=(
                    f"発行　+{natural_mint:,} S\n"
                    f"回収　-{natural_burn:,} S\n"
                    f"純増　{natural_sign}{natural_net:,} S"
                ),
                inline=False
            )
            if op_add > 0 or op_remove > 0:
                embed.add_field(
                    name="運営操作",
                    value=(
                        f"付与　+{op_add:,} S（{op_add_count}件）\n"
                        f"没収　-{op_remove:,} S（{op_remove_count}件）"
                    ),
                    inline=False
                )
            embed.set_footer(text=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Economy Report Error: {e}")
            traceback.print_exc()
            await interaction.followup.send(f"❌ レポート生成中にエラーが発生しました: {e}")

    # ── /市民ロール設定 ────────────────────────────────────
    @app_commands.command(name="市民ロール設定", description="【管理者】経済レポートの対象となる市民ロールを設定します")
    @app_commands.describe(role="市民ロール")
    @has_permission("SUPREME_GOD")
    async def set_citizen_role(self, interaction: discord.Interaction, role: discord.Role):
        async with self.bot.get_db() as db:
            await db.execute(
                "INSERT OR REPLACE INTO server_config (key, value) VALUES ('citizen_role_id', ?)",
                (str(role.id),)
            )
            await db.commit()
        await interaction.response.send_message(
            f"✅ 市民ロールを {role.mention} に設定しました。", ephemeral=True
        )


# ── 購入確認View ──
class ShopPurchaseView(discord.ui.View):
    def __init__(self, bot, role_id, price, shop_id, item_type, max_per_user):
        super().__init__(timeout=None)
        self.bot = bot
        self.role_id = role_id
        self.price = price
        self.shop_id = shop_id
        self.item_type = item_type          # 'rental' / 'permanent' / 'ticket'
        self.max_per_user = max_per_user

    def _button_label(self):
        if self.item_type == "rental":    return "購入する (30日間)"
        if self.item_type == "permanent": return "購入する (永続)"
        if self.item_type == "ticket":    return "購入する (引換券)"
        return "購入する"

    @discord.ui.button(style=discord.ButtonStyle.green, emoji="🛒")
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # ボタンラベルを動的に設定できないのでdeferしてから処理
        await interaction.response.defer(ephemeral=True)
        user = interaction.user

        # ── チケット枚数上限チェック ──
        if self.item_type == "ticket" and self.max_per_user > 0:
            async with self.bot.get_db() as db:
                async with db.execute(
                    "SELECT COUNT(*) as cnt FROM ticket_inventory WHERE user_id = ? AND item_key = ? AND used_at IS NULL",
                    (user.id, self.role_id)
                ) as c:
                    row = await c.fetchone()
                    if row['cnt'] >= self.max_per_user:
                        return await interaction.followup.send(
                            f"❌ このチケットは1人 **{self.max_per_user}枚** までしか持てません。\n（未使用チケットを先に使ってください）",
                            ephemeral=True
                        )

        # ── ロール系: 既に持っているか確認 ──
        if self.item_type in ("rental", "permanent"):
            role = interaction.guild.get_role(self.role_id)
            if not role:
                return await interaction.followup.send("❌ この商品は現在取り扱われていません。", ephemeral=True)
            if role in user.roles:
                return await interaction.followup.send(
                    f"❌ すでに **{role.name}** を持っています。",
                    ephemeral=True
                )

        # ── 残高チェック ──
        async with self.bot.get_db() as db:
            async with db.execute("SELECT balance FROM accounts WHERE user_id = ?", (user.id,)) as c:
                row = await c.fetchone()
                balance = row['balance'] if row else 0

        if balance < self.price:
            return await interaction.followup.send(
                f"❌ お金が足りません。\n(価格: {self.price:,} S / 所持金: {balance:,} S)",
                ephemeral=True
            )

        # ── 購入処理 ──
        month_tag = datetime.datetime.now().strftime("%Y-%m")
        try:
            async with self.bot.get_db() as db:
                await db.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE user_id = ?",
                    (self.price, user.id)
                )
                await db.execute(
                    "INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag) VALUES (?, 0, ?, 'SHOP', ?, ?)",
                    (user.id, self.price, f"購入: Shop({self.shop_id}) item({self.role_id})", month_tag)
                )

                if self.item_type == "rental":
                    expiry_date = datetime.datetime.now() + datetime.timedelta(days=30)
                    await db.execute(
                        "INSERT OR REPLACE INTO shop_subscriptions (user_id, role_id, expiry_date) VALUES (?, ?, ?)",
                        (user.id, self.role_id, expiry_date.strftime("%Y-%m-%d %H:%M:%S"))
                    )

                elif self.item_type == "ticket":
                    # チケットをインベントリに追加
                    async with db.execute(
                        "SELECT description FROM shop_items WHERE role_id = ? AND shop_id = ?",
                        (str(self.role_id), self.shop_id)
                    ) as c:
                        item_row = await c.fetchone()
                        item_name = item_row['description'] if item_row else "チケット"
                    await db.execute(
                        "INSERT INTO ticket_inventory (user_id, shop_id, item_key, item_name) VALUES (?, ?, ?, ?)",
                        (user.id, self.shop_id, str(self.role_id), item_name)
                    )

                await db.commit()

        except Exception as e:
            await db.rollback()
            return await interaction.followup.send(f"❌ エラーが発生しました: {e}", ephemeral=True)

        # ── ロール付与 ──
        if self.item_type in ("rental", "permanent"):
            try:
                role = interaction.guild.get_role(self.role_id)
                await user.add_roles(role, reason=f"ショップ購入({self.shop_id})")
                if self.item_type == "rental":
                    expiry_str = expiry_date.strftime('%Y/%m/%d')
                    msg = f"🎉 **購入完了！**\n**{role.name}** を購入しました。\n有効期限: **{expiry_str}** まで\n(-{self.price:,} S)"
                else:
                    msg = f"🎉 **購入完了！**\n**{role.name}** を永続付与しました。\n(-{self.price:,} S)"
                await interaction.followup.send(msg, ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send("⚠️ 購入処理は完了しましたが、権限不足でロールを付与できませんでした。", ephemeral=True)

        elif self.item_type == "ticket":
            await interaction.followup.send(
                f"🎟️ **チケット購入完了！**\n**{item_name}** を1枚取得しました。\n"
                f"管理者が確認し次第、特典が付与されます。\n(-{self.price:,} S)",
                ephemeral=True
            )


# ── 商品選択メニュー ──
class ShopSelect(discord.ui.Select):
    def __init__(self, bot, items, shop_id):
        self.bot = bot
        self.shop_id = shop_id

        TYPE_EMOJI = {"rental": "⏳", "permanent": "♾️", "ticket": "🎟️"}
        TYPE_LABEL = {"rental": "30日", "permanent": "永続", "ticket": "引換券"}

        options = []
        for item in items:
            t = item['item_type']
            label = f"{item['name']} ({item['price']:,} S)"
            desc = f"[{TYPE_LABEL.get(t, '?')}] {item['desc'] or '説明なし'}"
            options.append(discord.SelectOption(
                label=label[:100],
                description=desc[:100],
                value=str(item['role_id']),
                emoji=TYPE_EMOJI.get(t, "🏷️")
            ))
        super().__init__(
            placeholder="購入したい商品を選択してください...",
            min_values=1, max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        role_id_str = self.values[0]
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM shop_items WHERE role_id = ? AND shop_id = ?",
                (role_id_str, self.shop_id)
            ) as c:
                row = await c.fetchone()

        if not row:
            return await interaction.response.send_message("❌ 商品情報が取得できませんでした。", ephemeral=True)

        item_type = row['item_type'] or 'rental'
        price = row['price']
        max_per_user = row['max_per_user'] or 0
        role_id = int(role_id_str)

        TYPE_LABEL = {"rental": "30日レンタル", "permanent": "買い切り（永続）", "ticket": "引換券"}
        TYPE_EMOJI = {"rental": "⏳", "permanent": "♾️", "ticket": "🎟️"}

        if item_type in ("rental", "permanent"):
            role = interaction.guild.get_role(role_id)
            color = role.color if role else discord.Color.gold()
            name_str = role.mention if role else f"ID:{role_id}"
        else:
            color = discord.Color.purple()
            name_str = f"🎟️ {row['description'] or 'チケット'}"

        embed = discord.Embed(
            title=f"🛒 購入確認 ({TYPE_LABEL.get(item_type, '?')})",
            color=color
        )
        embed.add_field(name="商品", value=name_str, inline=False)
        embed.add_field(name="価格", value=f"**{price:,} Stell**", inline=True)
        embed.add_field(name="種別", value=f"{TYPE_EMOJI.get(item_type)} {TYPE_LABEL.get(item_type)}", inline=True)
        if item_type == "ticket" and max_per_user > 0:
            embed.add_field(name="所持上限", value=f"{max_per_user}枚まで", inline=True)

        view = ShopPurchaseView(self.bot, role_id, price, self.shop_id, item_type, max_per_user)
        # ボタンラベルをitem_typeに合わせて変更
        view.buy_button.label = view._button_label()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ShopPanelView(discord.ui.View):
    def __init__(self, bot, items, shop_id):
        super().__init__(timeout=None)
        self.add_item(ShopSelect(bot, items, shop_id))


# ── Cog本体 ──
class ShopSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_subscription_expiry.start()

    def cog_unload(self):
        self.check_subscription_expiry.cancel()

    @tasks.loop(hours=1)
    async def check_subscription_expiry(self):
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT user_id, role_id FROM shop_subscriptions WHERE expiry_date < ?", (now_str,)
            ) as cursor:
                expired_rows = await cursor.fetchall()

        if not expired_rows:
            return

        guild = self.bot.guilds[0]
        async with self.bot.get_db() as db:
            for row in expired_rows:
                member = guild.get_member(row['user_id'])
                role = guild.get_role(row['role_id'])
                if member and role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="ショップ有効期限切れ")
                        try:
                            await member.send(f"⏳ **有効期限切れ**\nロール **{role.name}** の有効期限（30日）が終了しました。")
                        except:
                            pass
                    except:
                        pass
                await db.execute(
                    "DELETE FROM shop_subscriptions WHERE user_id = ? AND role_id = ?",
                    (row['user_id'], row['role_id'])
                )
            await db.commit()

    @check_subscription_expiry.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()
    @app_commands.command(name="ショップ_商品登録", description="ショップに商品を登録します")
    @app_commands.rename(shop_id="ショップid", role="商品ロール", price="価格", description="説明文", item_type="種別", max_per_user="所持上限")
    @app_commands.describe(
        shop_id="配置するショップID（例: main）",
        role="対象のロール（チケットの場合は識別用に適当なロールを指定）",
        price="価格 (Stell)",
        description="商品説明文",
        item_type="rental=30日 / permanent=永続 / ticket=引換券",
        max_per_user="チケットの所持上限（0=無制限）"
    )
    @app_commands.choices(item_type=[
        app_commands.Choice(name="⏳ 期限付き (30日)", value="rental"),
        app_commands.Choice(name="♾️ 買い切り (永続)", value="permanent"),
        app_commands.Choice(name="🎟️ 引換券チケット", value="ticket"),
    ])
    @has_permission("SUPREME_GOD")
    async def shop_add(self, interaction: discord.Interaction, shop_id: str, role: discord.Role, price: int, description: str = None, item_type: str = "rental", max_per_user: int = 0):
        await interaction.response.defer(ephemeral=True)
        if price < 0:
            return await interaction.followup.send("❌ 価格は0以上にしてください。", ephemeral=True)

        async with self.bot.get_db() as db:
            await db.execute(
                "INSERT OR REPLACE INTO shop_items (role_id, shop_id, price, description, item_type, max_per_user) VALUES (?, ?, ?, ?, ?, ?)",
                (str(role.id), shop_id, price, description, item_type, max_per_user)
            )
            await db.commit()

        TYPE_LABEL = {"rental": "30日", "permanent": "永続", "ticket": "引換券"}
        await interaction.followup.send(
            f"✅ ショップ(`{shop_id}`) に **{role.name}** ({price:,} S / {TYPE_LABEL.get(item_type)}) を登録しました。",
            ephemeral=True
        )
    @app_commands.command(name="ショップ_商品削除", description="ショップから商品を取り下げます")
    @app_commands.rename(shop_id="ショップid", role="削除ロール")
    @app_commands.describe(shop_id="削除したい商品があるショップID", role="削除するロール")
    @has_permission("SUPREME_GOD")
    async def shop_remove(self, interaction: discord.Interaction, shop_id: str, role: discord.Role):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute(
                "DELETE FROM shop_items WHERE role_id = ? AND shop_id = ?",
                (str(role.id), shop_id)
            )
            await db.commit()
        await interaction.followup.send(f"🗑️ ショップ(`{shop_id}`) から **{role.name}** を削除しました。", ephemeral=True)
    @app_commands.command(name="ショップ_パネル設置", description="指定したIDのショップパネルを設置します")
    @app_commands.rename(shop_id="ショップid", title="タイトル", content="本文", image_url="画像url")
    @app_commands.describe(shop_id="表示するショップID", title="パネルタイトル", content="パネル本文", image_url="画像URL（任意）")
    @has_permission("SUPREME_GOD")
    async def shop_panel(self, interaction: discord.Interaction, shop_id: str, title: str = "🛒 ステラショップ", content: str = "欲しい商品を選択してください！", image_url: str = None):
        await interaction.response.defer()

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM shop_items WHERE shop_id = ?", (shop_id,)
            ) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            return await interaction.followup.send(f"❌ ショップID `{shop_id}` に商品がありません。", ephemeral=True)

        items = []
        TYPE_EMOJI = {"rental": "⏳", "permanent": "♾️", "ticket": "🎟️"}
        TYPE_LABEL = {"rental": "30日", "permanent": "永続", "ticket": "引換券"}
        item_list_text = ""

        for row in rows:
            role = interaction.guild.get_role(int(row['role_id']))
            if not role:
                continue
            t = row['item_type'] or 'rental'
            items.append({
                'role_id': int(row['role_id']),
                'name': role.name,
                'price': row['price'],
                'desc': row['description'],
                'item_type': t,
                'max_per_user': row['max_per_user'] or 0,
            })
            limit_str = f"（上限{row['max_per_user']}枚）" if t == "ticket" and row['max_per_user'] > 0 else ""
            item_list_text += f"{TYPE_EMOJI.get(t)} **{role.name}**: `{row['price']:,} S` [{TYPE_LABEL.get(t)}]{limit_str}\n"

        if not items:
            return await interaction.followup.send("❌ 有効な商品がありません。", ephemeral=True)

        embed = discord.Embed(title=title, description=content, color=Color.STELL)
        if image_url:
            embed.set_image(url=image_url)
        embed.add_field(name="📦 ラインナップ", value=item_list_text, inline=False)

        view = ShopPanelView(self.bot, items, shop_id)
        await interaction.followup.send(embed=embed, view=view)
    @app_commands.command(name="チケット確認", description="【管理者】未使用チケットの一覧を確認します")
    @app_commands.describe(shop_id="対象のショップID（省略で全件）")
    @has_permission("GODDESS")
    async def ticket_list(self, interaction: discord.Interaction, shop_id: str = None):
        await interaction.response.defer(ephemeral=True)

        async with self.bot.get_db() as db:
            if shop_id:
                async with db.execute(
                    "SELECT * FROM ticket_inventory WHERE used_at IS NULL AND shop_id = ? ORDER BY purchased_at ASC",
                    (shop_id,)
                ) as c:
                    rows = await c.fetchall()
            else:
                async with db.execute(
                    "SELECT * FROM ticket_inventory WHERE used_at IS NULL ORDER BY purchased_at ASC"
                ) as c:
                    rows = await c.fetchall()

        if not rows:
            return await interaction.followup.send("✅ 未使用チケットはありません。", ephemeral=True)

        embed = discord.Embed(
            title=f"🎟️ 未使用チケット一覧",
            description=f"{len(rows)}件",
            color=Color.CESTA
        )

        for row in rows:
            purchased = row['purchased_at'][:16] if row['purchased_at'] else "不明"
            embed.add_field(
                name=f"ID:{row['id']} | {row['item_name']}",
                value=f"所持者: <@{row['user_id']}>\n購入日: {purchased}",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)
    @app_commands.command(name="チケット処理済み", description="【管理者】チケットを処理済みにします")
    @app_commands.describe(ticket_id="チケットID（/チケット確認 で確認できます）")
    @has_permission("GODDESS")
    async def ticket_use(self, interaction: discord.Interaction, ticket_id: int):
        await interaction.response.defer(ephemeral=True)

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT * FROM ticket_inventory WHERE id = ?", (ticket_id,)
            ) as c:
                row = await c.fetchone()

            if not row:
                return await interaction.followup.send(f"❌ チケットID `{ticket_id}` が見つかりません。", ephemeral=True)
            if row['used_at']:
                return await interaction.followup.send(f"❌ チケットID `{ticket_id}` は既に処理済みです。", ephemeral=True)

            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await db.execute(
                "UPDATE ticket_inventory SET used_at = ?, used_by = ? WHERE id = ?",
                (now_str, interaction.user.id, ticket_id)
            )
            await db.commit()

        # 購入者にDM通知
        try:
            user = interaction.client.get_user(row['user_id']) or await interaction.client.fetch_user(row['user_id'])
            await user.send(
                f"🎟️ **チケット処理完了**\n"
                f"**{row['item_name']}** のチケット（ID: {ticket_id}）が処理されました。\n"
                f"特典付与をお待ちください。"
            )
        except:
            pass

        await interaction.followup.send(
            f"✅ チケットID `{ticket_id}` を処理済みにしました。\n"
            f"対象: <@{row['user_id']}> / 内容: **{row['item_name']}**",
            ephemeral=True
            )


# ── 3. 管理者ツール (整理版) ──
class AdminTools(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="ログ出力先決定", description="各ログの出力先を設定します")
    @app_commands.choices(log_type=[
        discord.app_commands.Choice(name="通貨ログ (送金など)", value="currency_log_id"),
        discord.app_commands.Choice(name="給与ログ (一斉支給)", value="salary_log_id"),
        discord.app_commands.Choice(name="面接ログ (合格通知)", value="interview_log_id"),
        discord.app_commands.Choice(name="削除ログ (メッセージ削除)", value="delete_log_id")
    ])
    @has_permission("SUPREME_GOD")
    async def config_log_channel(self, interaction: discord.Interaction, log_type: str, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES (?, ?)", (log_type, str(channel.id)))
            await db.commit()
        await self.bot.config.reload()
        await interaction.followup.send(f"✅ **{channel.mention}** をログ出力先に設定しました。", ephemeral=True)

    @app_commands.command(name="管理者権限設定", description="【オーナー用】管理権限ロールを登録・更新します")
    async def config_set_admin(self, interaction: discord.Interaction, role: discord.Role, level: str):
        await interaction.response.defer(ephemeral=True)
        if not await self.bot.is_owner(interaction.user):
            return await interaction.followup.send("オーナーのみ実行可能です。", ephemeral=True)
        
        valid_levels = ["SUPREME_GOD", "GODDESS", "ADMIN"]
        if level not in valid_levels:
             return await interaction.followup.send(f"レベルは {valid_levels} のいずれかである必要があります。", ephemeral=True)

        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO admin_roles (role_id, perm_level) VALUES (?, ?)", (role.id, level))
            await db.commit()
        await self.bot.config.reload()
        await interaction.followup.send(f"✅ {role.mention} を `{level}` に設定しました。", ephemeral=True)

    @app_commands.command(name="給与額設定", description="役職ごとの給与額を設定します")
    @has_permission("SUPREME_GOD")
    async def config_set_wage(self, interaction: discord.Interaction, role: discord.Role, amount: int):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO role_wages (role_id, amount) VALUES (?, ?)", (role.id, amount))
            await db.commit()
        await self.bot.config.reload()
        await interaction.followup.send(f"✅ 設定を更新しました。", ephemeral=True)

    @app_commands.command(name="vc報酬追加", description="報酬対象のVCを追加します")
    @has_permission("SUPREME_GOD")
    async def add_reward_vc(self, interaction: discord.Interaction, channel: discord.VoiceChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR IGNORE INTO reward_channels (channel_id) VALUES (?)", (channel.id,))
            await db.commit()
        
        vc_cog = self.bot.get_cog("VoiceSystem")
        if vc_cog: await vc_cog.reload_targets()
        await interaction.followup.send(f"✅ {channel.mention} を報酬対象に追加しました。", ephemeral=True)

    @app_commands.command(name="vc報酬解除", description="報酬対象のVCを解除します")
    @has_permission("SUPREME_GOD")
    async def remove_reward_vc(self, interaction: discord.Interaction, channel: discord.VoiceChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("DELETE FROM reward_channels WHERE channel_id = ?", (channel.id,))
            await db.commit()

        vc_cog = self.bot.get_cog("VoiceSystem")
        if vc_cog: await vc_cog.reload_targets()
        await interaction.followup.send(f"🗑️ {channel.mention} を報酬対象から除外しました。", ephemeral=True)

    @app_commands.command(name="vc報酬リスト", description="報酬対象のVC一覧を表示します")
    @has_permission("SUPREME_GOD")
    async def list_reward_vcs(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            async with db.execute("SELECT channel_id FROM reward_channels") as cursor:
                rows = await cursor.fetchall()
        
        if not rows: return await interaction.followup.send("報酬対象のVCは設定されていません。", ephemeral=True)
        channels_text = "\n".join([f"• <#{row['channel_id']}>" for row in rows])
        embed = discord.Embed(title="🎙 報酬対象VC一覧", description=channels_text, color=Color.SUCCESS)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        # Bot自身のメッセージ・DMは無視
        if message.author.bot:
            return
        if not message.guild:
            return

        log_ch_id = None
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'delete_log_id'") as c:
                row = await c.fetchone()
                if row:
                    log_ch_id = int(row['value'])

        if not log_ch_id:
            return

        channel = self.bot.get_channel(log_ch_id)
        if not channel:
            return

        embed = discord.Embed(
            title="🗑️ メッセージ削除ログ",
            color=Color.DANGER,
            timestamp=datetime.datetime.now()
        )
        embed.add_field(name="送信者", value=message.author.mention, inline=True)
        embed.add_field(name="チャンネル", value=message.channel.mention, inline=True)

        content = message.content or "*(テキストなし)*"
        if len(content) > 1000:
            content = content[:1000] + "…"
        embed.add_field(name="内容", value=content, inline=False)

        if message.attachments:
            att_list = "\n".join(a.filename for a in message.attachments)
            embed.add_field(name=f"添付ファイル ({len(message.attachments)}件)", value=att_list, inline=False)

        embed.set_footer(text=f"メッセージID: {message.id} | ユーザーID: {message.author.id}")
        embed.set_thumbnail(url=message.author.display_avatar.url)

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Delete Log Send Error: {e}")


    @app_commands.command(name="ギャンブル制限解除", description="【管理者】指定ユーザーまたはロールの今日のプレイ制限を解除します")
    @app_commands.describe(
        target="対象ユーザー（ロールと同時指定不可）",
        role="対象ロール（そのロールの全員を解除）",
        game="解除するゲーム"
    )
    @app_commands.choices(game=[
        app_commands.Choice(name="チンチロ", value="chinchiro"),
        app_commands.Choice(name="ブラックジャック", value="blackjack"),
        app_commands.Choice(name="両方", value="all"),
    ])
    @has_permission("ADMIN")
    async def lift_play_limit(self, interaction: discord.Interaction, game: str, target: Optional[discord.Member] = None, role: Optional[discord.Role] = None):
        await interaction.response.defer(ephemeral=True)

        if not target and not role:
            return await interaction.followup.send("❌ ユーザーかロールのどちらかを指定してください。", ephemeral=True)
        if target and role:
            return await interaction.followup.send("❌ ユーザーとロールは同時に指定できません。", ephemeral=True)

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        games = ["chinchiro", "blackjack"] if game == "all" else [game]

        # 対象メンバーリストを作成
        if target:
            members = [target]
        else:
            members = [m for m in role.members if not m.bot]
            if not members:
                return await interaction.followup.send(f"❌ {role.mention} にメンバーがいません。", ephemeral=True)

        async with self.bot.get_db() as db:
            for m in members:
                for g in games:
                    await db.execute("""
                        INSERT OR IGNORE INTO daily_play_exemptions (user_id, game, date)
                        VALUES (?, ?, ?)
                    """, (m.id, g, today))
            await db.commit()

        game_str = "チンチロ・ブラックジャック両方" if game == "all" else ("チンチロ" if game == "chinchiro" else "ブラックジャック")
        if target:
            msg = f"✅ {target.mention} の **{game_str}** の本日の制限を解除しました。"
        else:
            msg = f"✅ {role.mention} ({len(members)}名) の **{game_str}** の本日の制限を解除しました。"

        await interaction.followup.send(msg, ephemeral=True)

# ================================================================
#   UI: チケット作成パネル（TicketTool風ボタン式）
# ================================================================

async def _do_close_ticket(bot, interaction: discord.Interaction, ch: discord.TextChannel, ticket):
    """チケットのログ生成・DB更新・チャンネル削除を行う共通処理"""
    import io
    guild = interaction.guild

    log_lines = [
        "=== チケットログ ===",
        f"チケットID : {ch.id}",
        f"種類       : {ticket['type_name']}",
        f"作成者     : {ticket['user_id']}",
        f"作成日時   : {ticket['created_at']}",
        f"クローズ者 : {interaction.user} ({interaction.user.id})",
        f"クローズ日 : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 40, ""
    ]
    async for msg in ch.history(limit=None, oldest_first=True):
        ts   = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
        name = f"{msg.author.display_name} ({msg.author.id})"
        line = f"[{ts}] {name}: {msg.content or ''}"
        if msg.attachments:
            line += "\n  📎 " + " ".join(a.url for a in msg.attachments)
        log_lines.append(line)

    log_bytes = "\n".join(log_lines).encode("utf-8")
    log_file  = discord.File(
        fp=io.BytesIO(log_bytes),
        filename=f"ticket_{ch.id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    async with bot.get_db() as db:
        await db.execute(
            "UPDATE tickets SET closed_at = ?, closed_by = ? WHERE channel_id = ?",
            (datetime.datetime.now().isoformat(), interaction.user.id, ch.id)
        )
        await db.commit()
        async with db.execute("SELECT value FROM ticket_config WHERE key = 'log_channel_id'") as c:
            row = await c.fetchone()
    log_ch_id = int(row['value']) if row else None

    if log_ch_id:
        log_ch = bot.get_channel(log_ch_id)
        if log_ch:
            log_embed = discord.Embed(title="🔒 チケットクローズ", color=Color.DANGER, timestamp=datetime.datetime.now())
            log_embed.add_field(name="種類",     value=ticket['type_name'],         inline=True)
            log_embed.add_field(name="作成者",   value=f"<@{ticket['user_id']}>",   inline=True)
            log_embed.add_field(name="クローズ", value=interaction.user.mention,    inline=True)
            log_embed.add_field(name="作成日時", value=str(ticket['created_at'])[:16], inline=True)
            await log_ch.send(embed=log_embed, file=log_file)

    try:
        await ch.delete(reason=f"チケットクローズ by {interaction.user}")
    except Exception as e:
        logger.error(f"Ticket channel delete error: {e}")


class TicketCreateButton(discord.ui.Button):
    """パネルに並ぶ「チケット作成」ボタン（種類1つにつき1ボタン）"""
    def __init__(self, ticket_type: dict):
        super().__init__(
            label=ticket_type['name'],
            emoji=ticket_type['emoji'] or "🎫",
            style=discord.ButtonStyle.primary,
            custom_id=f"ticket_create_{ticket_type['id']}"
        )
        self.ticket_type_id = ticket_type['id']

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        bot   = interaction.client
        user  = interaction.user
        guild = interaction.guild

        async with bot.get_db() as db:
            async with db.execute("SELECT * FROM ticket_types WHERE id = ?", (self.ticket_type_id,)) as c:
                t = await c.fetchone()
            if not t:
                return await interaction.followup.send("❌ チケット種類が見つかりません。", ephemeral=True)
            async with db.execute("SELECT key, value FROM ticket_config") as c:
                cfg = {r['key']: r['value'] for r in await c.fetchall()}
            async with db.execute(
                "SELECT channel_id FROM tickets WHERE user_id = ? AND closed_at IS NULL", (user.id,)
            ) as c:
                existing = await c.fetchone()

        if existing:
            ch = guild.get_channel(existing['channel_id'])
            if ch:
                return await interaction.followup.send(f"❌ 既にチケットが開いています: {ch.mention}", ephemeral=True)

        category_id     = int(cfg['category_id'])     if 'category_id'     in cfg else None
        support_role_id = int(cfg['support_role_id']) if 'support_role_id' in cfg else None
        support_role    = guild.get_role(support_role_id) if support_role_id else None
        category        = guild.get_channel(category_id)  if category_id    else None

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me:           discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
            user:               discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        }
        if support_role:
            overwrites[support_role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True, manage_messages=True
            )

        emoji = t['emoji'] or "🎫"
        try:
            ch = await guild.create_text_channel(
                f"{emoji}│{user.display_name}",
                category=category, overwrites=overwrites
            )
        except Exception as e:
            logger.error(f"Ticket channel create error: {e}")
            return await interaction.followup.send("❌ チャンネル作成に失敗しました。", ephemeral=True)

        async with bot.get_db() as db:
            await db.execute(
                "INSERT INTO tickets (channel_id, user_id, type_name) VALUES (?, ?, ?)",
                (ch.id, user.id, t['name'])
            )
            await db.commit()

        embed = discord.Embed(
            title=f"{emoji} {t['name']}",
            description=(
                f"{user.mention} のチケットへようこそ！\n\n"
                f"担当スタッフ: {support_role.mention if support_role else '管理者'}\n\n"
                f"お問い合わせ内容を入力してください。\n"
                f"解決したら 🔒 **クローズ** を押してください。"
            ),
            color=Color.TICKET,
            timestamp=datetime.datetime.now()
        )
        embed.set_footer(text=f"チケットID: {ch.id}")

        await ch.send(
            content=f"{user.mention}" + (f" {support_role.mention}" if support_role else ""),
            embed=embed,
            view=TicketControlView()
        )
        await interaction.followup.send(f"✅ チケットを作成しました: {ch.mention}", ephemeral=True)


class TicketPanelView(discord.ui.View):
    def __init__(self, types: list):
        super().__init__(timeout=None)
        for t in types:
            self.add_item(TicketCreateButton(t))


class TicketControlView(discord.ui.View):
    """チケット内のコントロールパネル（担当・メンバー追加・クローズ）"""
    def __init__(self):
        super().__init__(timeout=None)

    async def _check_staff(self, interaction: discord.Interaction) -> bool:
        bot = interaction.client
        async with bot.get_db() as db:
            async with db.execute("SELECT value FROM ticket_config WHERE key = 'support_role_id'") as c:
                row = await c.fetchone()
        support_role_id = int(row['value']) if row else None
        support_role    = interaction.guild.get_role(support_role_id) if support_role_id else None
        is_support = support_role and support_role in interaction.user.roles
        is_admin   = await bot.is_owner(interaction.user) or any(r.id in bot.config.admin_roles for r in interaction.user.roles)
        return is_support or is_admin

    @discord.ui.button(label="担当する", style=discord.ButtonStyle.success, emoji="🙋", custom_id="ticket_claim_btn")
    async def claim_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = interaction.client
        async with bot.get_db() as db:
            async with db.execute("SELECT value FROM ticket_config WHERE key = 'support_role_id'") as c:
                row = await c.fetchone()
        support_role_id = int(row['value']) if row else None
        support_role    = interaction.guild.get_role(support_role_id) if support_role_id else None
        is_support = support_role and support_role in interaction.user.roles
        is_admin   = await bot.is_owner(interaction.user) or any(r.id in bot.config.admin_roles for r in interaction.user.roles)
        if not (is_support or is_admin):
            return await interaction.response.send_message("❌ スタッフロールがないと担当できません。", ephemeral=True)

        await interaction.response.send_message(f"✅ {interaction.user.mention} が担当します！", ephemeral=False)

    @discord.ui.button(label="メンバー追加", style=discord.ButtonStyle.secondary, emoji="➕", custom_id="ticket_add_member_btn")
    async def add_member_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_staff(interaction):
            return await interaction.response.send_message("❌ 権限がありません。", ephemeral=True)
        await interaction.response.send_message(
            "追加したいユーザーをメンションしてください（例: @ユーザー名）\n30秒以内に返信してください。",
            ephemeral=True
        )
        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id and m.mentions
        try:
            msg = await interaction.client.wait_for("message", check=check, timeout=30)
            for member in msg.mentions:
                await interaction.channel.set_permissions(
                    member,
                    view_channel=True, send_messages=True, read_message_history=True
                )
            names = ", ".join(m.display_name for m in msg.mentions)
            await interaction.channel.send(f"✅ {names} をチケットに追加しました。")
            try: await msg.delete()
            except: pass
        except asyncio.TimeoutError:
            pass

    @discord.ui.button(label="クローズ", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="ticket_close_btn")
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_staff(interaction):
            return await interaction.response.send_message("❌ クローズする権限がありません。", ephemeral=True)
        # 確認ダイアログ
        embed = discord.Embed(
            description="本当にこのチケットをクローズしますか？",
            color=Color.DANGER
        )
        await interaction.response.send_message(embed=embed, view=TicketCloseConfirmView(), ephemeral=True)


class TicketCloseConfirmView(discord.ui.View):
    """クローズ確認ダイアログ"""
    def __init__(self):
        super().__init__(timeout=30)

    @discord.ui.button(label="クローズする", style=discord.ButtonStyle.danger, emoji="🔒")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        bot = interaction.client
        ch  = interaction.channel
        async with bot.get_db() as db:
            async with db.execute("SELECT * FROM tickets WHERE channel_id = ? AND closed_at IS NULL", (ch.id,)) as c:
                ticket = await c.fetchone()
        if not ticket:
            return await interaction.followup.send("❌ チケット情報が見つかりません。", ephemeral=True)
        await _do_close_ticket(bot, interaction, ch, ticket)

    @discord.ui.button(label="キャンセル", style=discord.ButtonStyle.secondary, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("キャンセルしました。", ephemeral=True)
        self.stop()


# 旧TicketCloseViewとの互換用エイリアス
TicketCloseView = TicketControlView


class TicketSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # ── 設定コマンド群 ────────────────────────────────────

    @app_commands.command(name="チケット_カテゴリ設定", description="【管理者】チケットチャンネルを作るカテゴリを設定します")
    @has_permission("ADMIN")
    async def config_category(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO ticket_config (key, value) VALUES ('category_id', ?)", (str(category.id),))
            await db.commit()
        await interaction.response.send_message(f"✅ カテゴリを **{category.name}** に設定しました。", ephemeral=True)

    @app_commands.command(name="チケット_対応ロール設定", description="【管理者】チケットに対応するロールを設定します")
    @has_permission("ADMIN")
    async def config_support_role(self, interaction: discord.Interaction, role: discord.Role):
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO ticket_config (key, value) VALUES ('support_role_id', ?)", (str(role.id),))
            await db.commit()
        await interaction.response.send_message(f"✅ 対応ロールを {role.mention} に設定しました。", ephemeral=True)

    @app_commands.command(name="チケット_ログチャンネル設定", description="【管理者】クローズ時のログを送るチャンネルを設定します")
    @has_permission("ADMIN")
    async def config_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO ticket_config (key, value) VALUES ('log_channel_id', ?)", (str(channel.id),))
            await db.commit()
        await interaction.response.send_message(f"✅ ログチャンネルを {channel.mention} に設定しました。", ephemeral=True)

    @app_commands.command(name="チケット_種類追加", description="【管理者】チケットの種類を追加します")
    @app_commands.describe(name="種類名（例: 問い合わせ）", emoji="絵文字", description="説明文")
    @has_permission("ADMIN")
    async def add_ticket_type(self, interaction: discord.Interaction, name: str, emoji: str = "🎫", description: str = ""):
        async with self.bot.get_db() as db:
            try:
                await db.execute(
                    "INSERT INTO ticket_types (name, emoji, description) VALUES (?, ?, ?)",
                    (name, emoji, description)
                )
                await db.commit()
            except Exception:
                return await interaction.response.send_message(f"⚠️ **{name}** は既に登録されています。", ephemeral=True)
        await interaction.response.send_message(f"✅ チケット種類 {emoji} **{name}** を追加しました。", ephemeral=True)

    @app_commands.command(name="チケット_種類削除", description="【管理者】チケットの種類を削除します")
    @app_commands.describe(name="削除する種類名")
    @has_permission("ADMIN")
    async def remove_ticket_type(self, interaction: discord.Interaction, name: str):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT id FROM ticket_types WHERE name = ?", (name,)) as c:
                row = await c.fetchone()
            if not row:
                return await interaction.response.send_message(f"❌ **{name}** が見つかりません。", ephemeral=True)
            await db.execute("DELETE FROM ticket_types WHERE name = ?", (name,))
            await db.commit()
        await interaction.response.send_message(f"🗑️ チケット種類 **{name}** を削除しました。", ephemeral=True)

    @app_commands.command(name="チケット_種類一覧", description="【管理者】登録されているチケット種類を確認します")
    @has_permission("ADMIN")
    async def list_ticket_types(self, interaction: discord.Interaction):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT * FROM ticket_types ORDER BY id") as c:
                types = await c.fetchall()
        if not types:
            return await interaction.response.send_message("📝 チケット種類が登録されていません。", ephemeral=True)
        embed = discord.Embed(title="🎫 チケット種類一覧", color=Color.TICKET)
        for t in types:
            embed.add_field(
                name=f"{t['emoji']} {t['name']}",
                value=t['description'] or "説明なし",
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="チケット_パネル設置", description="【管理者】チケット作成パネルを設置します（種類を指定すると単独パネルも作れます）")
    @app_commands.describe(
        title="パネルタイトル",
        description="パネル説明文",
        種類名="特定の種類だけのパネルにしたい場合に入力（空欄=全種類）"
    )
    @has_permission("ADMIN")
    async def deploy_ticket_panel(
        self,
        interaction: discord.Interaction,
        title: str = "🎫 サポートチケット",
        description: str = "お問い合わせ・ご報告はチケットからお願いします。\nボタンを押してチケットを開いてください。",
        種類名: str = None
    ):
        await interaction.response.defer(ephemeral=True)

        async with self.bot.get_db() as db:
            if 種類名:
                async with db.execute("SELECT * FROM ticket_types WHERE name = ?", (種類名,)) as c:
                    types = await c.fetchall()
                if not types:
                    return await interaction.followup.send(f"❌ 種類「{種類名}」が見つかりません。`/チケット_種類一覧` で確認してください。", ephemeral=True)
            else:
                async with db.execute("SELECT * FROM ticket_types ORDER BY id") as c:
                    types = await c.fetchall()

        if not types:
            return await interaction.followup.send("❌ チケット種類が1つも登録されていません。先に /チケット_種類追加 で登録してください。", ephemeral=True)

        embed = discord.Embed(title=title, description=description, color=Color.TICKET)
        embed.set_footer(text=f"Last Updated: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M')}")

        await interaction.channel.send(embed=embed, view=TicketPanelView([dict(t) for t in types]))
        await interaction.followup.send(f"✅ チケットパネルを設置しました（{len(types)}種類）。", ephemeral=True)

    @app_commands.command(name="チケット_強制クローズ", description="【管理者】指定チャンネルのチケットを強制クローズします")
    @app_commands.describe(channel="クローズするチケットチャンネル")
    @has_permission("ADMIN")
    async def force_close_ticket(self, interaction: discord.Interaction, channel: discord.TextChannel):
        async with self.bot.get_db() as db:
            async with db.execute("SELECT * FROM tickets WHERE channel_id = ? AND closed_at IS NULL", (channel.id,)) as c:
                ticket = await c.fetchone()
        if not ticket:
            return await interaction.response.send_message("❌ 指定チャンネルにオープン中のチケットが見つかりません。", ephemeral=True)

        # TicketCloseViewのclose処理を流用
        view = TicketCloseView(self.bot)
        # interactionをチャンネルに差し替えて処理するため、直接処理を書く
        await interaction.response.defer(ephemeral=True)

        log_lines = [
            f"=== チケットログ (強制クローズ) ===",
            f"チケットID : {channel.id}",
            f"種類       : {ticket['type_name']}",
            f"作成者     : {ticket['user_id']}",
            f"作成日時   : {ticket['created_at']}",
            f"クローズ者 : {interaction.user} ({interaction.user.id})",
            f"クローズ日 : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 40,
            ""
        ]
        async for message in channel.history(limit=None, oldest_first=True):
            ts      = message.created_at.strftime("%Y-%m-%d %H:%M:%S")
            name    = f"{message.author.display_name} ({message.author.id})"
            content = message.content or ""
            attachments = " ".join(a.url for a in message.attachments)
            line = f"[{ts}] {name}: {content}"
            if attachments:
                line += f"\n  📎 {attachments}"
            log_lines.append(line)

        log_text  = "\n".join(log_lines)
        log_bytes = log_text.encode("utf-8")
        log_file  = discord.File(
            fp=__import__("io").BytesIO(log_bytes),
            filename=f"ticket_{channel.id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

        async with self.bot.get_db() as db:
            await db.execute(
                "UPDATE tickets SET closed_at = ?, closed_by = ? WHERE channel_id = ?",
                (datetime.datetime.now().isoformat(), interaction.user.id, channel.id)
            )
            await db.commit()

        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM ticket_config WHERE key = 'log_channel_id'") as c:
                row = await c.fetchone()
        log_ch_id = int(row['value']) if row else None

        if log_ch_id:
            log_ch = self.bot.get_channel(log_ch_id)
            if log_ch:
                embed = discord.Embed(title="🔒 チケットクローズ（強制）", color=Color.DANGER, timestamp=datetime.datetime.now())
                embed.add_field(name="種類",     value=ticket['type_name'],       inline=True)
                embed.add_field(name="作成者",   value=f"<@{ticket['user_id']}>", inline=True)
                embed.add_field(name="クローズ", value=interaction.user.mention,  inline=True)
                await log_ch.send(embed=embed, file=log_file)

        try:
            await channel.delete(reason=f"強制クローズ by {interaction.user}")
        except Exception as e:
            logger.error(f"Force close delete error: {e}")

        await interaction.followup.send("✅ チケットを強制クローズしました。", ephemeral=True)
class InterviewPanelView(discord.ui.View):
    def __init__(self, bot, routes, probation_role_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.routes = routes
        self.probation_role_id = probation_role_id
        self.selected_user = None

        # 対象者を選択するプルダウン
        self.add_item(InterviewUserSelect())

        # 登録されているルートボタンを動的に生成
        for slot, data in self.routes.items():
            btn = discord.ui.Button(
                label=data['desc'],
                emoji=data['emoji'],
                style=discord.ButtonStyle.primary,
                custom_id=f"eval_route_{slot}"
            )
            btn.callback = self.make_callback(slot, data)
            self.add_item(btn)

    def make_callback(self, slot, data):
        async def callback(interaction: discord.Interaction):
            if not self.selected_user:
                return await interaction.response.send_message("❌ 先に上のメニューから対象者(研修生)を選択してください。", ephemeral=True)

            await interaction.response.defer(ephemeral=True)
            member = interaction.guild.get_member(self.selected_user.id)
            if not member:
                return await interaction.followup.send("❌ 対象のユーザーがサーバーに見つかりません。", ephemeral=True)

            probation_role = interaction.guild.get_role(self.probation_role_id)
            new_role = interaction.guild.get_role(data['role_id'])
            bonus_amount = 30000
            month_tag = datetime.datetime.now().strftime("%Y-%m")

            try:
                # ロールの付け替え
                if probation_role and probation_role in member.roles:
                    await member.remove_roles(probation_role, reason="面接完了: 仮ロール削除")
                if new_role:
                    await member.add_roles(new_role, reason=f"面接完了: {data['desc']}ルート")

                # 祝金の付与
                async with self.bot.get_db() as db:
                    await db.execute("""
                        INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, ?, 0)
                        ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance
                    """, (member.id, bonus_amount))
                    
                    await db.execute("""
                        INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                        VALUES (0, ?, ?, 'BONUS', ?, ?)
                    """, (member.id, bonus_amount, f"面接合格: {data['desc']}", month_tag))
                    await db.commit()

                # ログ送信
                embed = discord.Embed(title="🌸 面接個別評価 完了", color=Color.STELL)
                embed.add_field(name="対象者", value=member.mention, inline=True)
                embed.add_field(name="決定ルート", value=f"{data['emoji']} {data['desc']}", inline=True)
                embed.add_field(name="付与ロール", value=new_role.mention if new_role else "なし", inline=False)
                embed.add_field(name="祝金", value=f"**{bonus_amount:,} Stell**", inline=False)
                embed.set_footer(text=f"担当面接官: {interaction.user.display_name}")

                log_ch_id = None
                async with self.bot.get_db() as db:
                    async with db.execute("SELECT value FROM server_config WHERE key = 'interview_log_id'") as c:
                        row = await c.fetchone()
                        if row: log_ch_id = int(row['value'])
                
                if log_ch_id:
                    log_ch = self.bot.get_channel(log_ch_id)
                    if log_ch: await log_ch.send(embed=embed)

                await interaction.followup.send(f"✅ **{member.display_name}** を **{data['desc']}** ルートで処理し、祝金を付与しました。", ephemeral=True)

            except Exception as e:
                logger.error(f"Interview Error: {e}")
                await interaction.followup.send(f"❌ 処理中にエラーが発生しました: {e}", ephemeral=True)

        return callback

# ── Cog: InterviewSystem (2段階評価システム) ──
class DynamicEvalView(discord.ui.View):
    def __init__(self, user_id, base_role_id, routes):
        super().__init__(timeout=None) # タイムアウトなしで2週間後でも押せるようにする
        
        # データベースに登録されているルートの数だけボタンを生成
        for slot, data in routes.items():
            btn = discord.ui.Button(
                label=data['desc'],
                emoji=data['emoji'],
                style=discord.ButtonStyle.primary,
                # custom_id に「ユーザーID」「剥奪する旧ロールID」「付与する新ロールID」を埋め込む（再起動対策）
                custom_id=f"eval_route:{user_id}:{base_role_id}:{data['role_id']}"
            )
            self.add_item(btn)


# ── Cog: RankingSystem (Probot代替) ──
class RankingSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._xp_cooldown: Dict[int, datetime.datetime] = {}  # {user_id: last_xp_time}

    @staticmethod
    def calc_level(xp: int) -> int:
        """XPからレベルを計算（Probot風）"""
        level = 0
        while xp >= RankingSystem.xp_for_next(level):
            xp -= RankingSystem.xp_for_next(level)
            level += 1
        return level

    @staticmethod
    def xp_for_next(level: int) -> int:
        """次のレベルに必要なXP"""
        return 5 * (level ** 2) + 50 * level + 100

    @staticmethod
    def xp_progress(total_xp: int):
        """現在レベル・現在XP・次レベル必要XPを返す"""
        level = 0
        remaining = total_xp
        while remaining >= RankingSystem.xp_for_next(level):
            remaining -= RankingSystem.xp_for_next(level)
            level += 1
        return level, remaining, RankingSystem.xp_for_next(level)

    @staticmethod
    def make_xp_bar(current: int, needed: int, length: int = 14) -> str:
        filled = int(length * current / needed) if needed > 0 else 0
        bar = "▰" * filled + "▱" * (length - filled)
        return bar

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot: return
        if not message.guild: return
        now = datetime.datetime.now()
        month_tag = now.strftime("%Y-%m")
        user_id = message.author.id

        # イースターエッグ: 「釈迦」を含むメッセージに0.5%で👁️リアクション
        if "釈迦" in message.content and random.random() < 0.005:
            try:
                await message.add_reaction("👁️")
            except Exception:
                pass

        try:
            async with self.bot.get_db() as db:
                # 月別メッセージカウント
                await db.execute(
                    "INSERT OR IGNORE INTO message_stats (user_id, month, count) VALUES (?, ?, 0)",
                    (user_id, month_tag)
                )
                await db.execute(
                    "UPDATE message_stats SET count = count + 1 WHERE user_id = ? AND month = ?",
                    (user_id, month_tag)
                )
                # XP加算（60秒クールダウン）
                last = self._xp_cooldown.get(user_id)
                if not last or (now - last).total_seconds() >= 60:
                    self._xp_cooldown[user_id] = now
                    xp_gain = random.randint(15, 25)
                    await db.execute(
                        "INSERT OR IGNORE INTO user_levels (user_id) VALUES (?)", (user_id,)
                    )
                    await db.execute(
                        "UPDATE user_levels SET xp = xp + ?, total_messages = total_messages + 1 WHERE user_id = ?",
                        (xp_gain, user_id)
                    )
                    async with db.execute("SELECT xp FROM user_levels WHERE user_id = ?", (user_id,)) as c:
                        row = await c.fetchone()
                    if row:
                        new_level = self.calc_level(row['xp'])
                        await db.execute("UPDATE user_levels SET level = ? WHERE user_id = ?", (new_level, user_id))
                else:
                    # クールダウン中でもメッセージ数は加算
                    await db.execute(
                        "INSERT OR IGNORE INTO user_levels (user_id) VALUES (?)", (user_id,)
                    )
                    await db.execute(
                        "UPDATE user_levels SET total_messages = total_messages + 1 WHERE user_id = ?",
                        (user_id,)
                    )
                await db.commit()
        except Exception as e:
            logger.error(f"Message Stats Error: {e}")

    @app_commands.command(name="ランク", description="自分のランクカードを表示します")
    async def rank(self, interaction: discord.Interaction):
        await interaction.response.defer()
        user = interaction.user
        month_tag = datetime.datetime.now().strftime("%Y-%m")

        async with self.bot.get_db() as db:
            # レベルデータ
            async with db.execute("SELECT xp, level, total_vc_seconds, total_messages FROM user_levels WHERE user_id = ?", (user.id,)) as c:
                lv_row = await c.fetchone()
            # 今月のVC時間
            async with db.execute("SELECT total_seconds FROM vc_rank_stats WHERE user_id = ? AND month = ?", (user.id, month_tag)) as c:
                vc_row = await c.fetchone()
            # 今月のメッセージ数
            async with db.execute("SELECT count FROM message_stats WHERE user_id = ? AND month = ?", (user.id, month_tag)) as c:
                msg_row = await c.fetchone()
            # サーバー内ランク順位
            async with db.execute("SELECT user_id FROM user_levels ORDER BY xp DESC") as c:
                all_users = await c.fetchall()

        total_xp = lv_row['xp'] if lv_row else 0
        total_vc_sec = lv_row['total_vc_seconds'] if lv_row else 0
        total_msgs = lv_row['total_messages'] if lv_row else 0
        month_vc_sec = vc_row['total_seconds'] if vc_row else 0
        month_msgs = msg_row['count'] if msg_row else 0

        level, current_xp, needed_xp = self.xp_progress(total_xp)
        xp_bar = self.make_xp_bar(current_xp, needed_xp)

        # 順位
        rank_pos = next((i + 1 for i, r in enumerate(all_users) if r['user_id'] == user.id), "?")

        # VC時間フォーマット
        def fmt_time(sec):
            h = sec // 3600
            m = (sec % 3600) // 60
            return f"{h}時間 {m}分"

        # ── ProBot風ランクカード Embed ──
        percent = int(current_xp / needed_xp * 100) if needed_xp > 0 else 0

        # メッセージランキング内の順位を取得
        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT user_id FROM message_stats WHERE month = ? ORDER BY count DESC", (month_tag,)
            ) as c:
                msg_rank_rows = await c.fetchall()
            async with db.execute(
                "SELECT user_id FROM vc_rank_stats WHERE month = ? ORDER BY total_seconds DESC", (month_tag,)
            ) as c:
                vc_rank_rows = await c.fetchall()

        msg_rank = next((i + 1 for i, r in enumerate(msg_rank_rows) if r['user_id'] == user.id), "?")
        vc_rank  = next((i + 1 for i, r in enumerate(vc_rank_rows)  if r['user_id'] == user.id), "?")

        vc_hours_total = total_vc_sec // 3600
        vc_mins_total  = (total_vc_sec % 3600) // 60

        embed = discord.Embed(color=0x5865F2)
        embed.set_author(name=f"✦ {user.display_name} のランクカード", icon_url=user.display_avatar.url)
        embed.set_thumbnail(url=user.display_avatar.url)

        # ── 区切り ──
        embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━", inline=False)

        # ── 💬 チャット ──
        embed.add_field(
            name="💬  チャット",
            value=(
                f"LVL **{level}**　·　Rank **#{rank_pos}**　·　Total XP: **{total_xp:,}**\n"
                f"`{xp_bar}`  {current_xp:,} / {needed_xp:,}  ({percent}%)\n"
                f"今月のメッセージ: **{month_msgs:,} 件**  (#{msg_rank})"
            ),
            inline=False
        )

        # ── 区切り ──
        embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━", inline=False)

        # ── 🎙️ ボイス ──
        embed.add_field(
            name="🎙️  ボイス",
            value=(
                f"Rank **#{vc_rank}**　·　今月: **{fmt_time(month_vc_sec)}**\n"
                f"累計: **{vc_hours_total}時間 {vc_mins_total}分**"
            ),
            inline=False
        )

        embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━", inline=False)
        embed.set_footer(text=f"集計月: {month_tag}")
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="縁", description="自分の縁リストを表示します")
    async def bond_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user = interaction.user

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT user_a, user_b, total_seconds, rank FROM bonds WHERE (user_a = ? OR user_b = ?) AND rank != '' AND rank != '__SELECT__' ORDER BY total_seconds DESC",
                (user.id, user.id)
            ) as c:
                rows = await c.fetchall()

        if not rows:
            embed = discord.Embed(description="まだ縁が結ばれていません。\nVCで誰かと5時間以上一緒にいると縁が生まれます。", color=Color.DARK)
            return await interaction.followup.send(embed=embed, ephemeral=True)

        embed = discord.Embed(title="― あなたの縁 ―", color=Color.DARK)
        lines = []
        for row in rows:
            other_id = row['user_b'] if row['user_a'] == user.id else row['user_a']
            member   = interaction.guild.get_member(other_id)
            name     = member.display_name if member else f"({other_id})"
            h = row['total_seconds'] // 3600
            m = (row['total_seconds'] % 3600) // 60
            lines.append(f"**{name}**　{row['rank']}\n　累計 {h}時間 {m}分")

        embed.description = "\n\n".join(lines)
        embed.set_footer(text=f"全{len(rows)}件")
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="メッセージランキング", description="今月のメッセージ数ランキングを表示します")
    @app_commands.describe(top="表示人数（デフォルト10人）")
    async def message_ranking(self, interaction: discord.Interaction, top: int = 10):
        await interaction.response.defer()
        top = max(1, min(top, 25))
        month_tag = datetime.datetime.now().strftime("%Y-%m")

        async with self.bot.get_db() as db:
            async with db.execute(
                "SELECT user_id, count FROM message_stats WHERE month = ? ORDER BY count DESC LIMIT ?",
                (month_tag, top)
            ) as cursor:
                rows = await cursor.fetchall()

        medals = ["🥇", "🥈", "🥉"]
        embed = discord.Embed(
            title="💬 メッセージランキング",
            description=f"集計期間: **{month_tag}**",
            color=Color.SUCCESS
        )
        if not rows:
            embed.add_field(name="データなし", value="まだ今月の記録がありません。", inline=False)
        else:
            lines = []
            for i, row in enumerate(rows):
                member = interaction.guild.get_member(row['user_id'])
                name = member.display_name if member else "退出済みユーザー"
                rank_label = medals[i] if i < 3 else f"`{i+1}.`"
                lines.append(f"{rank_label} **{name}** ── {row['count']:,} 件")
            embed.add_field(name="\u200b", value="\n".join(lines), inline=False)
        embed.set_footer(text=f"― {interaction.user.display_name}")
        await interaction.followup.send(embed=embed)


class InterviewSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # ── 1. 面接の基本設定 ──
    @app_commands.command(name="面接設定_ルート", description="【管理者】2週間後の評価分岐ルート(1〜5)を設定します")
    @app_commands.describe(slot="設定枠 (1~5)", role="付与するロール", emoji="ボタンの絵文字", description="ルート名（天使ルート等）")
    @app_commands.choices(slot=[app_commands.Choice(name=f"ルート {i}", value=i) for i in range(1, 6)])
    @has_permission("SUPREME_GOD")
    async def config_eval_branch(self, interaction: discord.Interaction, slot: int, role: discord.Role, emoji: str, description: str):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES (?, ?)", (f"branch_{slot}_role", str(role.id)))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES (?, ?)", (f"branch_{slot}_emoji", emoji))
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES (?, ?)", (f"branch_{slot}_desc", description))
            await db.commit()
        await interaction.followup.send(f"✅ **ルート {slot}** を設定しました。\n{emoji} {description} ➡ {role.mention}", ephemeral=True)

    @app_commands.command(name="評価パネル送信先設定", description="【管理者】VC面接通過後、2週間後の評価パネルを送るチャンネルを設定します")
    @has_permission("SUPREME_GOD")
    async def config_eval_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('eval_channel_id', ?)", (str(channel.id),))
            await db.commit()
        await interaction.followup.send(f"✅ VC面接通過後の「評価待ちパネル」を {channel.mention} に送信するよう設定しました。", ephemeral=True)

    # ── 2. 除外ロールの管理 (複数対応) ──
    @app_commands.command(name="面接除外_追加", description="【管理者】VC一括合格の対象から外すロール(面接官など)を追加します")
    @has_permission("SUPREME_GOD")
    async def add_exclude_role(self, interaction: discord.Interaction, role: discord.Role):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'interview_exclude_roles'") as c:
                row = await c.fetchone()
                current = row['value'].split(',') if row and row['value'] else []
            
            if str(role.id) not in current:
                current.append(str(role.id))
                await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('interview_exclude_roles', ?)", (','.join(current),))
                await db.commit()
                await interaction.followup.send(f"✅ {role.mention} を除外ロールに追加しました。", ephemeral=True)
            else:
                await interaction.followup.send(f"⚠️ {role.mention} は既に除外ロールに登録されています。", ephemeral=True)

    @app_commands.command(name="面接除外_削除", description="【管理者】登録されている除外ロールを解除します")
    @has_permission("SUPREME_GOD")
    async def remove_exclude_role(self, interaction: discord.Interaction, role: discord.Role):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'interview_exclude_roles'") as c:
                row = await c.fetchone()
                current = row['value'].split(',') if row and row['value'] else []
            
            if str(role.id) in current:
                current.remove(str(role.id))
                await db.execute("INSERT OR REPLACE INTO server_config (key, value) VALUES ('interview_exclude_roles', ?)", (','.join(current),))
                await db.commit()
                await interaction.followup.send(f"🗑️ {role.mention} を除外ロールから削除しました。", ephemeral=True)
            else:
                await interaction.followup.send(f"⚠️ {role.mention} は除外ロールに登録されていません。", ephemeral=True)

    @app_commands.command(name="面接除外_一覧", description="【管理者】現在登録されている除外ロールの一覧を確認します")
    @has_permission("ADMIN")
    async def list_exclude_roles(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'interview_exclude_roles'") as c:
                row = await c.fetchone()
                current = row['value'].split(',') if row and row['value'] else []

        if not current:
            return await interaction.followup.send("📝 除外ロールは登録されていません。", ephemeral=True)

        mentions = [f"<@&{role_id}>" for role_id in current]
        embed = discord.Embed(title="🛡️ 面接除外ロール一覧", description="\n".join(mentions), color=Color.TICKET)
        await interaction.followup.send(embed=embed, ephemeral=True)


    # ── 3. 実行コマンド: VC一括面接 (Phase 1) ──
    @app_commands.command(name="面接_vc一括合格", description="【管理者】VC内の対象者を合格させ、2週間後の評価パネルを自動生成します")
    @app_commands.describe(target_role="変更前のロール(Aロール)", new_role="変更後のロール(Bロール)")
    @has_permission("ADMIN")
    async def pass_interview_vc(self, interaction: discord.Interaction, target_role: discord.Role, new_role: discord.Role):
        if not interaction.user.voice or not interaction.user.voice.channel:
            return await interaction.response.send_message("❌ VCに参加してから実行してください。", ephemeral=True)
        
        channel = interaction.user.voice.channel
        await interaction.response.defer(ephemeral=True)

        exclude_roles = []
        eval_channel_id = None
        routes = {}

        # DBから設定を読み込む
        async with self.bot.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = 'interview_exclude_roles'") as c:
                row = await c.fetchone()
                if row and row['value']: exclude_roles = [int(x) for x in row['value'].split(',')]
            
            async with db.execute("SELECT value FROM server_config WHERE key = 'eval_channel_id'") as c:
                row = await c.fetchone()
                if row: eval_channel_id = int(row['value'])

            for i in range(1, 6):
                async with db.execute("SELECT key, value FROM server_config WHERE key LIKE ?", (f"branch_{i}_%",)) as c:
                    rows = await c.fetchall()
                    data = {}
                    for r in rows:
                        if r['key'].endswith('_role'): data['role_id'] = int(r['value'])
                        elif r['key'].endswith('_emoji'): data['emoji'] = r['value']
                        elif r['key'].endswith('_desc'): data['desc'] = r['value']
                    if 'role_id' in data: routes[i] = data

        processed_members = []
        bonus_amount = 30000
        month_tag = datetime.datetime.now().strftime("%Y-%m")

        # 対象者のロール付け替えと祝金付与
        async with self.bot.get_db() as db:
            for member in channel.members:
                if member.bot: continue
                if any(r.id in exclude_roles for r in member.roles): continue
                if target_role not in member.roles: continue

                try:
                    await member.remove_roles(target_role, reason="面接一括合格: Aロール削除")
                    await member.add_roles(new_role, reason="面接一括合格: Bロール付与")
                    
                    await db.execute("""
                        INSERT INTO accounts (user_id, balance, total_earned) VALUES (?, ?, 0)
                        ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance
                    """, (member.id, bonus_amount))
                    
                    await db.execute("""
                        INSERT INTO transactions (sender_id, receiver_id, amount, type, description, month_tag)
                        VALUES (0, ?, ?, 'BONUS', '面接一括合格祝い', ?)
                    """, (member.id, bonus_amount, month_tag))
                    
                    processed_members.append(member)
                except Exception as e:
                    logger.error(f"Interview Error: {e}")
            await db.commit()

        if not processed_members:
            return await interaction.followup.send("⚠️ 対象となるメンバーがいませんでした。", ephemeral=True)

        # 実行者(自分)への結果報告（Ephemeral）
        embed = discord.Embed(title="🌸 VC面接 合格処理完了", color=Color.SUCCESS)
        embed.add_field(name="処理人数", value=f"{len(processed_members)} 名", inline=False)
        embed.add_field(name="ロール変更", value=f"{target_role.mention} ➡ {new_role.mention}", inline=False)
        names = ", ".join([m.display_name for m in processed_members])
        embed.add_field(name="対象者", value=names[:1000], inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

        # 指定チャンネルへ評価パネル(備忘録)を送信
        if eval_channel_id and routes:
            eval_ch = self.bot.get_channel(eval_channel_id)
            if eval_ch:
                for member in processed_members:
                    view = DynamicEvalView(member.id, new_role.id, routes)
                    msg_embed = discord.Embed(
                        title=f"📋 評価待ち: {member.display_name}", 
                        description=f"現在のロール: {new_role.mention}\n2週間後、決定したルートのボタンを押してください。",
                        color=Color.DARK
                    )
                    msg_embed.set_thumbnail(url=member.display_avatar.url)
                    await eval_ch.send(content=f"{member.mention}", embed=msg_embed, view=view)


    # ── 4. ボタンが押された時の処理 (Phase 2: 2週間後の評価) ──
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        # コンポーネント(ボタン)じゃなければ無視
        if interaction.type != discord.InteractionType.component: return
        
        custom_id = interaction.data.get("custom_id", "")
        # 面接の評価ボタンじゃなければ無視
        if not custom_id.startswith("eval_route:"): return

        # eval_route:{user_id}:{base_role_id}:{new_role_id} の形式で情報を抽出
        parts = custom_id.split(":")
        if len(parts) != 4: return
        
        target_id = int(parts[1])
        base_role_id = int(parts[2])
        new_role_id = int(parts[3])

        await interaction.response.defer(ephemeral=True)

        member = interaction.guild.get_member(target_id)
        if not member:
            return await interaction.followup.send("❌ ユーザーが既にサーバーにいないようです。", ephemeral=True)

        base_role = interaction.guild.get_role(base_role_id)
        new_role = interaction.guild.get_role(new_role_id)

        try:
            # ロールの付け替え (Bロールを剥奪して、C/Dロールを付与)
            if base_role and base_role in member.roles:
                await member.remove_roles(base_role, reason="2週間評価: Bロール剥奪")
            if new_role:
                await member.add_roles(new_role, reason="2週間評価: ルート確定ロール付与")

            # 押したボタンのあるメッセージを更新(ボタンを消して完了済みにする)
            completed_embed = interaction.message.embeds[0]
            completed_embed.color = discord.Color.gold()
            completed_embed.title = f"✅ 評価完了: {member.display_name}"
            completed_embed.description = f"決定ルート: {new_role.mention if new_role else '不明'}\n担当: {interaction.user.display_name}"
            
            # ビューを空にしてメッセージを更新
            await interaction.message.edit(embed=completed_embed, view=None)
            await interaction.followup.send(f"✅ {member.display_name} の評価を完了し、ロールを更新しました。", ephemeral=True)

        except Exception as e:
            logger.error(f"Eval Error: {e}")
            await interaction.followup.send("❌ ロールの変更中にエラーが発生しました。権限などを確認してください。", ephemeral=True)


# ── Bot 本体 ──
class CestaBankBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.voice_states = True
        intents.message_content = True
        
        super().__init__(
            command_prefix="!", 
            intents=intents,
            help_command=None
        )
        
        self.db_path = "stella_bank_v1.db"
        self.db_manager = BankDatabase(self.db_path)
        self.config = ConfigManager(self)

    @contextlib.asynccontextmanager
    async def get_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute("PRAGMA busy_timeout = 5000")
            yield db

    async def setup_hook(self):
        async with self.get_db() as db:
            await self.db_manager.setup(db)
            # ジャックポット用
            await db.execute("""CREATE TABLE IF NOT EXISTS jackpot_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                ticket_id TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            # 統計レポート用
            await db.execute("""CREATE TABLE IF NOT EXISTS last_stats_report (
                id INTEGER PRIMARY KEY, 
                total_balance INTEGER, 
                gini_val REAL, 
                timestamp DATETIME
            )""")
            await db.commit()
        
        await self.config.reload()
        
        if 'VCPanel' in globals():
            self.add_view(VCPanel())
            self.add_view(PublicVCPanel())

        # チケットパネルの永続化
        async with self.get_db() as db:
            async with db.execute("SELECT * FROM ticket_types") as c:
                types = await c.fetchall()
        if types:
            self.add_view(TicketPanelView([dict(t) for t in types]))
        self.add_view(TicketControlView())
        
        await self.add_cog(Economy(self))
        await self.add_cog(Salary(self))
        await self.add_cog(AdminTools(self))
        await self.add_cog(ServerStats(self))
        await self.add_cog(ShopSystem(self))
        await self.add_cog(HumanStockMarket(self))

        await self.add_cog(VoiceSystem(self))
        await self.add_cog(PrivateVCManager(self))
        await self.add_cog(VoiceHistory(self))
        await self.add_cog(InterviewSystem(self))
        await self.add_cog(RankingSystem(self))

        await self.add_cog(Jackpot(self))
        await self.add_cog(Omikuji(self))
        await self.add_cog(CestaSystem(self))
        await self.add_cog(CestaShop(self))
        await self.add_cog(Chinchiro(self))
        await self.add_cog(Blackjack(self))
        await self.add_cog(Countdown(self))
        await self.add_cog(TicketSystem(self))
        
        if not self.backup_db_task.is_running():
            self.backup_db_task.start()
        
        await self.tree.sync()
        logger.info("StellaBank System: Setup complete and All Cogs Synced.")

    async def send_bank_log(self, log_key: str, embed: discord.Embed):
        """
        指定されたキー（currency_log_id, salary_log_id 等）の設定を読み込み、
        対応するチャンネルへログを送信します。
        """
        async with self.get_db() as db:
            async with db.execute("SELECT value FROM server_config WHERE key = ?", (log_key,)) as c:
                row = await c.fetchone()
                if row:
                    try:
                        channel_id = int(row['value'])
                        channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
                        if channel:
                            await channel.send(embed=embed)
                    except Exception as e:
                        logger.error(f"Log Send Error ({log_key}): {e}")

    @tasks.loop(hours=24)
    async def backup_db_task(self):

        # 1. 新しいバックアップを作成
        backup_name = f"backup_{datetime.datetime.now().strftime('%Y%m%d')}.db"
        try:
            async with self.get_db() as db:
                await db.execute(f"VACUUM INTO '{backup_name}'")
            
            logger.info(f"Auto Backup Success: {backup_name}")

            # 2. 古いバックアップを削除 (最新3世代のみ残す)
            # "backup_*.db" に一致するファイルをすべて取得して、名前順(日付順)に並べる
            backups = sorted(glob.glob("backup_*.db"))
            
            # バックアップが3つより多い場合、古いものから削除する
            if len(backups) > 3:
                # リストの「後ろから3つ」を除いたもの（＝古いファイル）を対象にループ
                for old_bk in backups[:-3]:
                    try:
                        os.remove(old_bk) # ファイル削除
                        logger.info(f"Deleted old backup: {old_bk}")
                    except Exception as e:
                        logger.error(f"Failed to delete {old_bk}: {e}")

        except Exception as e:
            logger.error(f"Backup Failure: {e}")

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info("--- Stella Bank System Online ---")
        
# ── 実行ブロック ──
if __name__ == "__main__":
    if not TOKEN:
        logging.error("DISCORD_TOKEN is missing")
    else:
        # ボットの起動
        bot = CestaBankBot()
        bot.run(TOKEN)
