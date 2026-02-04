import aiosqlite
import logging
import datetime

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger('SharedDB')

DB_PATH = "lumen_bank_v4.db"

class Database:
    """
    銀行BotとカジノBotで共有して使うデータベース接続クラス。
    経済・VC・カジノ・エンタメ・アイテム管理・BAN機能を含む完全版。
    """
    def __init__(self):
        self.db_path = DB_PATH

    async def get_connection(self):
        """DB接続を取得し、WALモード等の設定を行う"""
        conn = await aiosqlite.connect(self.db_path, timeout=30.0)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        return conn

    async def setup(self):
        """
        テーブル作成の真・完全版。
        アイテム管理(Inventory)と凍結機能(Ban)を追加。
        """
        async with await self.get_connection() as db:
            # ==========================================
            # 1. 銀行・経済システム
            # ==========================================
            await db.execute("""CREATE TABLE IF NOT EXISTS accounts (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0 CHECK(balance >= 0), 
                total_earned INTEGER DEFAULT 0
            )""")
            
            await db.execute("""CREATE TABLE IF NOT EXISTS transactions (
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

            # 設定・権限・給与
            await db.execute("CREATE TABLE IF NOT EXISTS server_config (key TEXT PRIMARY KEY, value TEXT)")
            await db.execute("CREATE TABLE IF NOT EXISTS role_wages (role_id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)")
            await db.execute("CREATE TABLE IF NOT EXISTS admin_roles (role_id INTEGER PRIMARY KEY, perm_level TEXT)")
            await db.execute("CREATE TABLE IF NOT EXISTS user_settings (user_id INTEGER PRIMARY KEY, dm_salary_enabled INTEGER DEFAULT 1)")
            
            # ▼▼▼ 追加機能1: Bot利用禁止リスト（口座凍結） ▼▼▼
            await db.execute("""CREATE TABLE IF NOT EXISTS bot_bans (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                banned_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")

            # ==========================================
            # 2. VC報酬システム
            # ==========================================
            await db.execute("CREATE TABLE IF NOT EXISTS voice_stats (user_id INTEGER PRIMARY KEY, total_seconds INTEGER DEFAULT 0)")
            await db.execute("CREATE TABLE IF NOT EXISTS voice_tracking (user_id INTEGER PRIMARY KEY, join_time TEXT)")
            await db.execute("""CREATE TABLE IF NOT EXISTS temp_vcs (
                channel_id INTEGER PRIMARY KEY,
                guild_id INTEGER,
                owner_id INTEGER,
                expire_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            await db.execute("CREATE TABLE IF NOT EXISTS reward_channels (channel_id INTEGER PRIMARY KEY)")

            # ==========================================
            # 3. カジノ・ショップ・アイテム系
            # ==========================================
            await db.execute("CREATE TABLE IF NOT EXISTS jackpot_tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ticket_id TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
            
            # ショップ商品定義
            await db.execute("CREATE TABLE IF NOT EXISTS shop_items (role_id TEXT, shop_id TEXT, price INTEGER, description TEXT, PRIMARY KEY (role_id, shop_id))")
            # ロール（サブスク）管理
            await db.execute("CREATE TABLE IF NOT EXISTS shop_subscriptions (user_id INTEGER, role_id INTEGER, expiry_date TEXT, PRIMARY KEY (user_id, role_id))")

            # ▼▼▼ 追加機能2: アイテムインベントリ（所持品） ▼▼▼
            await db.execute("""CREATE TABLE IF NOT EXISTS user_inventory (
                user_id INTEGER,
                item_id TEXT,
                quantity INTEGER DEFAULT 0 CHECK(quantity >= 0),
                PRIMARY KEY (user_id, item_id)
            )""")

            # ==========================================
            # 4. 統計・レポート・エンタメ管理
            # ==========================================
            await db.execute("CREATE TABLE IF NOT EXISTS daily_stats (date TEXT PRIMARY KEY, total_balance INTEGER)")
            await db.execute("CREATE TABLE IF NOT EXISTS last_stats_report (id INTEGER PRIMARY KEY, total_balance INTEGER, gini_val REAL, timestamp DATETIME)")
            
            # リアクションロール
            await db.execute("""CREATE TABLE IF NOT EXISTS reaction_roles (
                message_id INTEGER,
                emoji TEXT,
                role_id INTEGER,
                PRIMARY KEY (message_id, emoji)
            )""")

            # クールダウン
            await db.execute("""CREATE TABLE IF NOT EXISTS cooldowns (
                user_id INTEGER,
                command_name TEXT,
                last_used_at DATETIME,
                PRIMARY KEY (user_id, command_name)
            )""")

            # ゲームマスターデータ
            await db.execute("""CREATE TABLE IF NOT EXISTS game_master_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                content TEXT,
                added_by INTEGER
            )""")

            # インデックス
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trans_receiver ON transactions (receiver_id, created_at DESC)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_temp_vc_expire ON temp_vcs (expire_at)")
            
            await db.commit()
            logger.info("✅ Database setup complete (Final Version).")

    # ==========================================================
    # ▼▼▼ ここから下が新しく追加した「共通機能」です ▼▼▼
    # ==========================================================

    async def get_balance(self, user_id: int) -> int:
        """【共通】ユーザーの残高を取得する"""
        async with await self.get_connection() as conn:
            async with conn.execute("SELECT balance FROM accounts WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return row['balance'] if row else 0

    async def update_balance(self, user_id: int, amount: int, transaction_type: str, description: str, batch_id: str = None) -> bool:
        """
        【共通】残高の増減と履歴保存を安全に行う最強のメソッド。
        銀行BotもカジノBotも、お金を動かす時はこれを使うだけでOK。
        
        Args:
            user_id: ユーザーID
            amount: 増減額（マイナスなら支払い、プラスなら収入）
            transaction_type: 'SLOT', 'SALARY', 'TRANSFER' など
            description: 履歴に載る説明文
        
        Returns:
            bool: 成功ならTrue、残高不足やエラーならFalse
        """
        async with await self.get_connection() as conn:
            try:
                # 1. ユーザー口座がなければ自動作成 (初期値0)
                await conn.execute("INSERT OR IGNORE INTO accounts (user_id, balance, total_earned) VALUES (?, 0, 0)", (user_id,))
                
                # 2. 支払い(マイナス)の場合、残高が足りるかチェック
                if amount < 0:
                    async with conn.execute("SELECT balance FROM accounts WHERE user_id = ?", (user_id,)) as cursor:
                        row = await cursor.fetchone()
                        if not row or row['balance'] < abs(amount):
                            return False # 残高不足で失敗

                # 3. 残高を更新
                # プラスの時だけ total_earned (生涯獲得額) も増やす
                earn_add = amount if amount > 0 else 0
                
                await conn.execute("""
                    UPDATE accounts 
                    SET balance = balance + ?, total_earned = total_earned + ?
                    WHERE user_id = ?
                """, (amount, earn_add, user_id))

                # 4. 取引履歴を保存
                month_tag = datetime.datetime.now().strftime("%Y-%m")
                # amountがマイナスでも、履歴にはそのまま記録（例: -500）
                # receiver_id は「お金が動いた対象」として記録
                await conn.execute("""
                    INSERT INTO transactions (receiver_id, amount, type, description, batch_id, month_tag)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (user_id, amount, transaction_type, description, batch_id, month_tag))

                await conn.commit()
                return True # 成功

            except Exception as e:
                logger.error(f"Transaction Error ({transaction_type}): {e}")
                await conn.rollback()
                raise e # Bot側でエラーハンドリングさせるために例外を投げる

    async def is_banned(self, user_id: int) -> bool:
        """【共通】ユーザーがBot利用禁止(BAN)されているか確認"""
        async with await self.get_connection() as conn:
            async with conn.execute("SELECT 1 FROM bot_bans WHERE user_id = ?", (user_id,)) as cursor:
                return await cursor.fetchone() is not None

# インスタンスを作成しておく（他のファイルからは from database import db で使える）
db = Database()
