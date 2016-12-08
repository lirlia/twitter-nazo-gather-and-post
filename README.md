# How to use (for myself)

```
$ virtualenv uploader
$ source  uploader/bin/activate
$ pip install -r requirements.txt
$ pip install lambda-uploader # 最新のリリース版をインストール
$ pip install git+https://github.com/rackerlabs/lambda-uploader # git main branch をインストール
$ aws configure
AWS Access Key ID [None]: xx
AWS Secret Access Key [None]: xx
Default region name [ap-northeast-1]: ap-northeast-1
Default output format [None]:
$ lambda-uploader
λ Building Package
λ Uploading Package
λ Fin
```

[AWS Lambda Pythonをlambda-uploaderでデプロイ ｜ Developers.IO](http://dev.classmethod.jp/cloud/deploy-aws-lambda-python-with-lambda-uploader/)
