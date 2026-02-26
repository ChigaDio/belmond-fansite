from youtubedataapi import YoutubeDataFind, YoutubeOrder, YoutubeVideoDetail,get_youtube_data
import argparse
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import ConfigurationError, PyMongoError
import urllib.parse
from datetime import datetime
from typing import List, Optional
import traceback

class ArgsKey:
    api_key: str = "None"
    channel_id: str = "None"
    mongo_base_uri: str = "None"
    mongo_user: str = "None"
    mongo_password: str = "None"
    db_name: str = "youtube_data"

def build_mongo_uri(base_uri: str, user: str, password: str) -> str:
    """mongodb+srv:// 形式のベースURIにユーザー名・パスワードを安全に挿入"""
    if not base_uri.startswith("mongodb+srv://"):
        raise ValueError("mongo_base_uri は 'mongodb+srv://' で始まる必要があります（例: mongodb+srv://cluster0.abcde.mongodb.net/）")

    encoded_user = urllib.parse.quote_plus(user)
    encoded_pass = urllib.parse.quote_plus(password)

    # mongodb+srv://[user:pass@]host...
    if "@" in base_uri:
        # 既にユーザー情報が入っている場合は警告
        print("警告: mongo_base_uri に既にユーザー名/パスワードが含まれているようです。base_uriはユーザー抜きで指定してください。")
    
    # ホスト部分の前に挿入
    uri = base_uri.replace("mongodb+srv://", f"mongodb+srv://{encoded_user}:{encoded_pass}@", 1)
    
    # クエリパラメータがなければ追加（安定性のため）
    if "?" not in uri:
        uri += "?retryWrites=true&w=majority"
    elif "retryWrites" not in uri:
        uri += "&retryWrites=true&w=majority"
    
    return uri

def get_mongo_client(uri: str) -> Optional[MongoClient]:
    try:
        client = MongoClient(
            uri,
            server_api=ServerApi('1'),
            connectTimeoutMS=15000,
            serverSelectionTimeoutMS=15000
        )
        # 接続テスト
        client.admin.command('ping')
        print("MongoDB Atlas に接続成功しました")
        return client
    except ConfigurationError as e:
        print(f"接続設定エラー（URIやネットワークを確認）: {e}")
        return None
    except Exception as e:
        print(f"MongoDB接続中に予期しないエラー: {e}")
        traceback.print_exc()
        return None

def ensure_indexes(db):
    """必要最低限のインデックスを作成（高速検索・重複防止のため）"""
    try:
        videos_coll = db["videos"]
        videos_coll.create_index([("channel_name", 1), ("published_at", -1)])
        videos_coll.create_index("_id", unique=True)
        
        channels_coll = db["channels"]
        channels_coll.create_index("channel_id", unique=True)
        
        print("インデックス作成/確認完了")
    except PyMongoError as e:
        print(f"インデックス作成中にエラー（既存なら無視可）: {e}")

def save_to_mongodb(
    client: MongoClient,
    channel_id: str,
    db_name: str,
    youtubeuser,
    videos: List[YoutubeVideoDetail]
):
    if not client:
        print("MongoDBクライアントが無効です。保存をスキップします")
        return

    db = client[db_name]
    ensure_indexes(db)  # 初回実行時にインデックス作成

    # ── 1. チャンネル情報保存 ──
    channels_coll = db["channels"]
    
    channel_filter = {"channel_id": channel_id}  # 本来は channel_id を使うべき（nameは重複可能性あり）
    channel_doc = {
        "channel_id": channel_id,  # ← ここを実際のchannel_idに変えるとbetter
        "name": youtubeuser.name,
        "followers": youtubeuser.followers,
        "last_updated": datetime.utcnow(),
        "source": "youtube_data_api",
        "last_fetched": datetime.utcnow()
    }

    channels_coll.update_one(
        channel_filter,
        {"$set": channel_doc},
        upsert=True
    )
    
    print(f"チャンネル情報を保存/更新しました: {youtubeuser.name} ({youtubeuser.followers} subscribers)")


    # ── 2. 動画情報保存（Bulkで効率的に） ──
    videos_coll = db["videos"]
    operations = []

    for video in videos:
        # None値を適切に扱う
        doc = {
            "_id": video.video_id,
            "channel_name": youtubeuser.name,
            "title": video.title,
            "published_at": video.published_at,
            "view_count": video.view_count if video.view_count is not None else 0,
            "like_count": video.like_count if video.like_count is not None else 0,
            "comment_count": video.comment_count if video.comment_count is not None else 0,
            "duration_sec": video.duration_sec if video.duration_sec is not None else 0.0,
            "url": video.url,
            "content_category": video.content_category.value if video.content_category else "unknown",
            "is_holiday": video.is_holiday,
            "weekday": video.weekday.value if video.weekday else None,
            "consecutive_broadcast_days": video.consecutive_broadcast_days,
            "same_day_broadcast_count": video.same_day_broadcast_count,
            "days_since_last_broadcast": video.days_since_last_broadcast,
            "was_broadcast_yesterday": video.was_broadcast_yesterday,
            "live_status": video.live_status,
            "is_live_now": video.is_live_now,
            "concurrent_viewers": video.concurrent_viewers if video.concurrent_viewers is not None else 0,
            "scheduled_start_time": video.scheduled_start_time,
            "actual_start_time": video.actual_start_time,
            "actual_end_time": video.actual_end_time,
            "last_updated": datetime.now()
        }

        operations.append(
            UpdateOne(
                {"_id": video.video_id},
                {"$set": doc},
                upsert=True
            )
        )

    if operations:
        try:
            result = videos_coll.bulk_write(operations, ordered=False)
            print(f"動画保存結果:")
            print(f"  - 挿入（新規）   : {result.upserted_count} 件")
            print(f"  - 更新（既存）   : {result.modified_count} 件")
            print(f"  - 合計処理件数   : {len(operations)} 件")
        except PyMongoError as e:
            print(f"Bulk write エラー: {e}")
            traceback.print_exc()
    else:
        print("保存する動画がありません")

def main():
    
    
    parser = argparse.ArgumentParser(
        description="YouTube Data API で動画情報を取得し、MongoDB Atlas に保存します（URIを短く保つ設計）"
    )
    parser.add_argument("--api_key", "-api", type=str, required=True, help="YouTube Data APIキー")
    parser.add_argument("--channel_id", "-c", type=str, required=True, help="対象のYouTubeチャンネルID")
    
    parser.add_argument("--mongo_base_uri", "-mu", type=str, required=True,
                        help="MongoDB AtlasのベースURI（ユーザー/パスワード抜き）例: mongodb+srv://cluster0.abcde.mongodb.net/")
    parser.add_argument("--mongo_user", "-muu", type=str, required=True, help="MongoDBのユーザー名（例: write_fan）")
    parser.add_argument("--mongo_password", "-mup", type=str, required=True, help="MongoDBのパスワード")
    
    parser.add_argument("--db_name", "-dbn", type=str, default="youtube_data", help="使用するデータベース名（デフォルト: youtube_data）")

    args = parser.parse_args()

    # MongoDB URI 構築
    try:
        full_uri = build_mongo_uri(args.mongo_base_uri, args.mongo_user, args.mongo_password)
        print(f"構築したURI（パスワード部分は隠蔽）: {full_uri.replace(args.mongo_password, '********')}")
    except ValueError as e:
        print(f"URI構築エラー: {e}")
        return

    client = get_mongo_client(full_uri)
    if not client:
        return

    # YouTube データ取得
    find = YoutubeDataFind(
        Api=args.api_key,
        ChannelId=args.channel_id,
        MaxResults=0  # 0 = 制限なし（全部取得）
    )

    result = get_youtube_data(find)
    
    if result is None or not isinstance(result, tuple) or len(result) != 2:
        print("YouTube データ取得に失敗しました")
        client.close()
        return
    

    youtubeuser, videos = result
    
    if youtubeuser is None:
        print("チャンネル情報が取得できませんでした（channel_idを確認）")
        client.close()
        return
    
    if not videos:
        print("動画が1件も見つかりませんでした")
        client.close()
        return

    print(f"\n取得完了: {len(videos)} 本の動画データ")

    # MongoDB に保存
    save_to_mongodb(client, args.channel_id, args.db_name, youtubeuser, videos)

    client.close()
    print("\nすべての処理が完了しました")

if __name__ == "__main__":
    main()