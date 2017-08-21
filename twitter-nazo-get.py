# -*- coding: utf-8 -*-

from requests_oauthlib import OAuth1Session
import os
import sys
import urllib
import json
import datetime
from pytz import timezone
from dateutil import parser
import boto3
import random
import hashlib
import base64
import requests
from xml.sax.saxutils import *

## Twitter系の変数
# OAuth認証 セッションを開始
CK = os.getenv('Twitter_Consumer_Key')          # Consumer Key
CS = os.getenv('Twitter_Consumer_Secret_Key')   # Consumer Secret
AT = os.getenv('Twitter_Access_Token_Key')      # Access Token
AS = os.getenv('Twitter_Access_Token_Secret')   # Accesss Token Secert

twitter = OAuth1Session(CK, CS, AT, AS)

twitterListID = 795464117347721216  # TwitterリストID
twitterListCount = 500              # 一度に取得するリストのアカウント数
twitterFav = 20                     # 集計対象RT数
twitterRT = 20                      # 集計対象Fav数

## DynamoDB系の変数
tableNameSequence = 'sequences'             # AtomicCounter用のテーブル名
tableSequensesColumnName = 'my_table'       # AtomicCounter用のカラム名
tableTweetName = 'nazo-tweet-tables'        # Twitter謎格納用のテーブル名

#テスト用
#tableNameSequence = 'sequences-test'
#tableTweetName = 'nazo-tweet-tables-test'
regionName = 'ap-northeast-1'               # 使用するリージョン名

dynamodb = boto3.resource(
    'dynamodb',
    region_name = regionName,
    aws_access_key_id = os.getenv("AWS_Access_Key_Id"),
    aws_secret_access_key = os.getenv("AWS_Secret_Access_Key")
)

## Hatena系の変数
hatenaUsername = 'lirlia'
hatenaPassword = os.environ.get('Hatena_Password')
hatenaBlogname = 'lirlia.hatenablog.com'
hatenaDraft = 'yes'

## その他の変数
today = datetime.date.today()
lastWeek = datetime.datetime.today() - datetime.timedelta(days=7)

#
# 特定の条件を満たすTweetを検索
# 引数：twitterのid(tenhouginsama)
# 戻り値: 条件を満たしたツイートの検索結果
# https://dev.twitter.com/rest/reference/get/search/tweets
#
def SearchTweet(twitterScreenName):

    url = "https://api.twitter.com/1.1/search/tweets.json"

    searchWord = \
        'since:' + lastWeek.strftime("%Y-%m-%d") + '_12:00:00_JST ' \
        'until:' + today.strftime("%Y-%m-%d") + '_11:59:59_JST ' \
        'from:' + str(twitterScreenName) + ' ' \
        'min_faves:' + str(twitterFav) + ' ' \
        'min_retweets:' + str(twitterRT)

    params = {'q': searchWord }
    req = twitter.get(url, params = params)

    # レスポンスを確認
    if req.status_code != 200:
        print ("Error: %d" % req.status_code)
        sys.exit()

    return json.loads(req.text)

#
# 集計対象のTwitterリストに存在するユーザー情報を取得
# 戻り値： Twitter Listのusers以下のjson
# https://dev.twitter.com/rest/reference/get/lists/members
#
def GetTwitterAccount():

    url = 'https://api.twitter.com/1.1/lists/members.json'
    params = {"list_id": twitterListID, "count": twitterListCount}

    # OAuth認証で GET method で投稿
    req = twitter.get(url, params = params)

    # レスポンスを確認
    if req.status_code != 200:
        print ("Error: %d" % req.status_code)
        sys.exit()

    return json.loads(req.text)['users']

#
# DynamoDBへTweet情報を格納
#
def InsertDynamoDB(num, tweet):
    #
    # TwitterではUTCでTweetが管理されているのでJSTへ変換
    # DynamoDBへはstr型での取り込みとなるため変換
    #
    date = str(parser.parse(tweet['created_at']).astimezone(timezone('Asia/Tokyo')))

    table = dynamodb.Table(tableTweetName)
    response = table.put_item(
        Item={
             'no': int(num),
             'name': tweet['user']['name'],
             'id': tweet['user']['screen_name'],
             'tweet_id': tweet['id_str'],
             'text': tweet['text'],
             'date': date,
             'retweet_count': tweet['retweet_count'],
             'favorite_count': tweet['favorite_count']
        }
    )

#
# AtomicSequence（対象のTweetを格納する時のユニークな番号を用意する）
# 戻り値：DBにインサートするときに使用するユニークな番号
#
def Sequence():
    table = dynamodb.Table(tableNameSequence)
    response = table.update_item(
        Key={
             'name': tableSequensesColumnName
        },
        UpdateExpression="set current_number = current_number + :val",
        ExpressionAttributeValues={
        ':val': 1
        },
        ReturnValues="UPDATED_NEW"
    )

    import decimal
    return str(decimal.Decimal(response['Attributes']['current_number']))

#
# WSSE認証の取得
#
def Wsse():
    created = datetime.datetime.now().isoformat() + 'Z'
    nonce = hashlib.sha1(str(random.random())).digest()
    digest = hashlib.sha1(nonce + created + hatenaPassword).digest()

    return 'UsernameToken Username="{}", PasswordDigest="{}", Nonce="{}", Created="{}"'.format(hatenaUsername, base64.b64encode(digest), base64.b64encode(nonce), created)

#
# HatenaBlogへの記事の投稿
#
def PostHatena(nazoList):

    day1 = lastWeek.strftime("%Y/%m/%d")
    day2 = today.strftime("%Y/%m/%d")

    title = u'今週Twitterで話題だった謎を紹介！(' \
        + day1 + u'〜' + day2 + u') #週謎';

    body = \
        u'<p><img class="hatena-fotolife" title="画像" src="https://cdn-ak.f.st-hatena.com/images/fotolife/l/lirlia/20140206/20140206190730.jpg" alt="f:id:lirlia:20161124194747j:plain" /></p>' \
        u'<p><!-- more --></p>' \
        u'<p></p>' \
        u'<p>こんにちは、<span id="myicon"> </span><a href="https://twitter.com/intent/user?original_referer=http://lirlia.hatenablog.com/&amp;region=follow&amp;screen_name=tenhouginsama&amp;tw_p=followbutton&amp;variant=2.0">ぎん</a>です。' \
        u'<p></p>' \
        u'<p>' + \
        day1 + u'〜' + day2 + u'の期間に、人気を集めた謎をご紹介します。</p><br>' \
        u'<p></p>' \
        u'<p>[:contents]</p>' \
        u'<p></p>' \
        u'<h3>今週人気のTwitter謎一覧</h3>'

    for i in nazoList:
        body = body + u'<h4>' + i['userName'] + u' (RT:' + str(i['rt']) + u' Fav:' + str(i['fav']) + u')</h4>' + \
      u'<p>[https://twitter.com/' + i['twitterID'] + u'/status/' + str(i['tweetID']) + u':embed]</p>'

    body = body +  \
        u'<h3>Twitter謎の収集について</h3>' \
        u'<h4>これまで紹介したTwitter謎</h4>' \
        u'<p>いままでに紹介したTwitter謎はこちらから↓</p>' \
        u'<p><iframe class="embed-card embed-webcard" style="display: block; width: 100%; height: 155px; max-width: 500px; margin: 10px 0px;" title="Twitter謎 カテゴリーの記事一覧 - なぞまっぷ" src="https://hatenablog-parts.com/embed?url=http%3A%2F%2Fwww.nazomap.com%2Farchive%2Fcategory%2FTwitter%25E8%25AC%258E" frameborder="0" scrolling="no"></iframe></p>' \
        u'<p></p>' \
        u'<h4>集計の条件</h4>' \
        u'<p>Twitterから話題の謎を集める条件はこちらです</p>' \
        u'<ul>' \
        u'<li>' + day1 + u' 12:00:00(JST) 〜' + day2 + u' 11:59:59(JST) の期間に投稿された新作謎であること。</li>' \
        u'<li>データ集計タイミング(' + day2 + u' 21:00)にRTが' + str(twitterRT) + u'以上であること。</li>' \
        u'<li>データ集計タイミング(' + day2 + u' 21:00)にお気に入り数が' + str(twitterFav) + u'以上であること。</li>' \
        u'<li>RT数、お気に入り数の条件は今後変動の可能性があります。</li>' \
        u'<li>自分自身のアカウントによって該当ツイートが集計タイミング時点でRTされていないこと。</li></ul>' \
        u'<p></p>' \
        u'<h4>集計対象Twitterアカウント</h4>' \
        u'<p>集計対象アカウントは下記のTwitterリストとなります。</p>' \
        u'<ul><li>https://twitter.com/tenhouginsama/lists/twitter-nazo/members</li></ul>' \
        u'<p></p>' \
        u'<p><strong>「このアカウントも収集対象に追加して欲しい」</strong>というご要望があれば[https://twitter.com/intent/user?original_referer=http%3A%2F%2Flirlia.hatenablog.com%2F&amp;region=follow&amp;screen_name=tenhouginsama&amp;tw_p=followbutton&amp;variant=2.0:title=(@tenhouginsama)]までご連絡ください</p>'
    body = escape(body)

    data = \
        u'<?xml version="1.0" encoding="utf-8"?>' \
        u'<entry xmlns="http://www.w3.org/2005/Atom"' \
        u'xmlns:app="http://www.w3.org/2007/app">' \
        u'<title>' + title + '</title>' \
        u'<author><name>name</name></author>' \
        u'<content type="text/plain">' + body + '' \
        u'</content>' \
        u'<category term="練習問題" />' \
        u'<category term="練習問題-小謎" />' \
        u'<category term="Twitter謎" />' \
        u'<app:control>' \
        u'<app:draft>' + hatenaDraft + '</app:draft>' \
        u'</app:control>' \
        u'</entry>'

    headers = {'X-WSSE': Wsse()}
    url = 'http://blog.hatena.ne.jp/{}/{}/atom/entry'.format(hatenaUsername, hatenaBlogname)
    req = requests.post(url, data=data.encode('utf-8'), headers=headers)

    if req.status_code != 201:
        print ("Error: %d" % req.status_code)
        sys.exit()

def lambda_handler(event, context):

    nazoList = []

    # リストからアカウントを検索
    for account in GetTwitterAccount():

        # 対象のアカウントのツイートから条件を満たしているものを抽出
        for tweet in SearchTweet(account['screen_name'])['statuses']:

            # 取得したツイートが条件を満たしていない場合があるので排除する
            if int(tweet['retweet_count']) < twitterRT or \
                int(tweet['favorite_count']) < twitterFav:
                continue

            # DynamoDBへの格納の処理
            #InsertDynamoDB(Sequence(), tweet)

            # データの格納
            nazoList.append({
                'userName': tweet['user']['name'],
                'tweetID': tweet['id_str'],
                'twitterID': tweet['user']['screen_name'],
                'rt': tweet['retweet_count'],
                'fav': tweet['favorite_count']
            })

    # ブログへ記事を投稿
    PostHatena(nazoList)

    return { "messages":"success!" }

lambda_handler(1,1)
