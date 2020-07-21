/*
業務毎（機能枚）に共通処理な処理などを、この層に実装する。

例えば
・API Gateway から呼び出すLambdaに共通
・SQSを呼び出すLambdaに共通
など、用途毎に共通となるような処理は、この層に実装すると良い

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function AbstractS3PutTriggerCommon() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractBizCommon");
  // prototypeにセットする事で継承関係のように挙動させる
  AbstractS3PutTriggerCommon.prototype = new superClazzFunc();

  // ワーカー処理同時実行数
  var EXECUTORS_THREADS_NUM = "1";
  if (process && process.env && process.env.ExecutorsThreadsNum) {
    EXECUTORS_THREADS_NUM = process.env.ExecutorsThreadsNum;
  }

  // ワーカー処理トランザクション制御（１周毎にwaitする：ミリ秒指定）
  var EXECUTORS_THREADS_WAIT = "0";
  if (process && process.env && process.env.ExecutorsThreadsWait) {
    EXECUTORS_THREADS_WAIT = process.env.ExecutorsThreadsWait;
  }

  // 処理の実行
  function* executeBizUnitCommon(event, context, bizRequireObjects) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# executeBizUnitCommon : start"
      );

      // 自動再実行ようの演算
      var reCallCount = event.reCallCount || 0;
      reCallCount += 1;
      event.reCallCount = reCallCount;

      // 読み込みモジュールの引き渡し
      AbstractS3PutTriggerCommon.prototype.RequireObjects = bizRequireObjects;

      // 親の業務処理を実行
      return yield AbstractS3PutTriggerCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# executeBizUnitCommon : end"
      );
    }
  }
  AbstractS3PutTriggerCommon.prototype.executeBizUnitCommon = executeBizUnitCommon;

  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.getExecutorsThreadsNum = function () {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getExecutorsThreadsNum : start"
      );
      return parseInt(EXECUTORS_THREADS_NUM);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getExecutorsThreadsNum : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon); // AbstractS3PutTriggerCommonをthisとする

  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.getExecutorsThreadsWait = function () {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getExecutorsThreadsWait : start"
      );
      return parseInt(EXECUTORS_THREADS_WAIT);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getExecutorsThreadsWait : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon); // AbstractS3PutTriggerCommonをthisとする

  /*
  S3アクセスパラメータ初期化

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.initS3PutTriggerParameter = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# initS3PutTriggerParameter : start"
      );

      var bucket = args.Records[0].s3.bucket.name;
      var keyName = args.Records[0].s3.object.key;

      var s3params = {
        Bucket: bucket,
        Key: keyName,
      };

      return new Promise(function (resolve, reject) {
        resolve(s3params);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# initS3PutTriggerParameter : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  S3アクセスパラメータ初期化

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.waitExistsS3Object = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# waitExistsS3Object : start"
      );

      var s3params = base.getLastIndexObject(args);

      return new Promise(function (resolve, reject) {
        var loggingFromDate;
        var loggingToDate;

        var retryLimitMax = base.getObjectWaitForRetryMax();
        var retryCount = 0;

        // リトライ用にコールバック関数で定義
        var s3CallBack = function (err, data) {
          loggingToDate = base.getCurrentDate();

          var loggingFromDateStr = base.getTimeStringJst9(loggingFromDate);
          var loggingToDateStr = base.getTimeStringJst9(loggingToDate);
          var diffsec = loggingToDate.getTime() - loggingFromDate.getTime();

          var msg =
            "AbstractS3PutTriggerCommon# WaitFor From:" +
            loggingFromDateStr +
            " To:" +
            loggingToDateStr +
            "# Diff:" +
            diffsec;
          base.writeLogInfo(msg);

          if (err) {
            retryCount++;
            base.printStackTrace(err);
            base.writeLogInfo(
              "AbstractS3PutTriggerCommon# Object Exists Retry:" + retryCount
            );

            // 計測用初期化
            loggingFromDate = base.getCurrentDate();
            loggingToDate = base.getCurrentDate();

            if (retryCount < retryLimitMax) {
              base.RequireObjects.S3.waitFor(
                "objectExists",
                s3params,
                s3CallBack
              );
            } else {
              base.writeLogWarn(
                "AbstractS3PutTriggerCommon# Object Exists RetryMax Over:" +
                  retryCount
              );
              reject(err);
            }
          } else {
            resolve(s3params);
          }
        };

        // 計測用初期化
        loggingFromDate = base.getCurrentDate();
        loggingToDate = base.getCurrentDate();

        // S3オブジェクトの存在確認（アクセス可能になるのを待つ）
        base.RequireObjects.S3.waitFor("objectExists", s3params, s3CallBack);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# waitExistsS3Object : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  業務前処理：S3から/tmpへファイルを取得する

  @param arg 実行結果配列（最初の処理は、Lambdaの起動引数：event)
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# beforeMainExecute :start"
      );

      // S3 アクセス用のパラメータ取得
      var s3params = base.getFirstIndexObject(args);
      var keyName = s3params.Key;

      base.writeLogInfo("AbstractS3PutTriggerCommon# S3 Stream Read Start");

      return new Promise(function (resolve, reject) {
        // 読み込みストリーム
        var s3stream = base.RequireObjects.S3.getObject(
          s3params
        ).createReadStream();

        // 出力ストリーム
        var fs = base.RequireObjects.Fs;
        var writeStream = fs.createWriteStream("/tmp/" + keyName, {
          flags: "w",
        });

        // pipeで繋ぐ
        s3stream.pipe(writeStream);

        // 出力終了
        s3stream.on("end", function () {
          resolve("Save Local File");
        });

        // 読み込みエラー時
        s3stream.on("error", function (err) {
          base.printStackTrace(err);
          // エラーの時は閉じられないので一応閉める
          writeStream.end();
          reject(err);
        });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractS3PutTriggerCommon# beforeMainExecute :end");
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  業務前処理：/tmpに保存したPDFファイルを読み込んで処理単位の配列を組み立てる

  @param arg 実行結果配列（最初の処理は、Lambdaの起動引数：event)
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.pdfParse = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    var s3params = base.getFirstIndexObject(args);
    var keyName = s3params.Key;
    try {
      base.writeLogTrace("AbstractS3PutTriggerCommon# pdfParse :start");

      var Hummus = base.RequireObjects.Hummus;

      var pdfReader = Hummus.createReader("/tmp/" + keyName);
      var pagesCount = pdfReader.getPagesCount();

      var MemoryStream = base.RequireObjects.MemoryStream;
      var memoryWriter = new MemoryStream.WritableStream();
      var pdfWriter = Hummus.createWriter(
        new Hummus.PDFStreamForResponse(memoryWriter)
      );

      var outputS3params = {};
      outputS3params.Bucket = "pdftestsave";
      // outputS3params.Key = "tmp.pdf";
      // outputS3params.Body = readStream;

      var baseKeyName = String(keyName);
      baseKeyName = baseKeyName.replace(/.pdf$/, "");
      baseKeyName = baseKeyName + "_";

      return new Promise(function (resolve, reject) {
        base.writeLogTrace("AbstractS3PutTriggerCommon# Loop Before");
        var targets = [];

        for (var i = 0; i < pagesCount; i++) {
          var target = {
            pdfReader,
            baseKeyName,
            pageNo: i,
          };

          targets.push(target);
        }

        var result = { workerArgs: targets };
        resolve(result);
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractS3PutTriggerCommon# pdfParse :end");
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  業務メイン処理

  N件あるデータをワーカー処理（疑似スレッド処理）に引き渡し実行制御をする。

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# businessMainExecute : start"
      );

      // 直前のPromise処理の結果取得
      var beforePromiseResult = base.getLastIndexObject(args);

      //　ワーカー処理のクラス取得
      var subWorkerClazzFunc = base.getSubWorkerClazzFunc();

      var datas = beforePromiseResult.workerArgs || [];

      return new Promise(function (resolve, reject) {
        var executorsErrArray = [];
        var executorsResultArray = [];

        function sleep(msec, val) {
          return new Promise(function (resolve, reject) {
            setTimeout(resolve, msec, val);
          });
        }

        function* sub(record, threadsWait) {
          try {
            var workerFuncObj = new subWorkerClazzFunc();

            var result = yield workerFuncObj.execute(
              record,
              base.promiseRefs.context,
              base.RequireObjects
            );
            executorsResultArray.push(result);

            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractS3PutTriggerCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
          } catch (catchErr) {
            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractS3PutTriggerCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
            executorsErrArray.push(catchErr);
          }
        }

        function* controller(datas) {
          var results = [];
          try {
            var threadsNum = base.getExecutorsThreadsNum();
            var threadsWait = base.getExecutorsThreadsWait();
            var executors = base.RequireObjects.Executors(threadsNum);

            for (var i = 0; i < datas.length; i++) {
              var record = datas[i];
              results.push(executors(sub, record, threadsWait));
            }

            // Worker処理（疑似スレッド）の待ち合わせ
            yield results;

            // エラーハンドリング
            if (executorsErrArray.length > 0) {
              var irregularErr;
              for (var i = 0; i < executorsErrArray.length; i++) {
                var executorsErr = executorsErrArray[i];
                base.printStackTrace(executorsErr);

                if (!base.judgeBizError(executorsErr)) {
                  irregularErr = executorsErr;
                }
              }
              if (irregularErr) {
                throw irregularErr;
              }
            }

            return executorsResultArray;
          } catch (catchErr) {
            yield results;
            throw catchErr;
          }
        }

        base.RequireObjects.aa(controller(datas))
          .then(function (results) {
            resolve(results);
          })
          .catch(function (err) {
            reject(err);
          });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  Tmpに保存した一時ファイルを削除する

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.afterTmpDeleteExecute = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# afterTmpDeleteExecute : start"
      );

      var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
      var s3params = base.getFirstIndexObject(args);
      var keyName = s3params.Key;

      var fs = base.RequireObjects.Fs;
      return new Promise(function (resolve, reject) {
        fs.unlink("/tmp/" + keyName, function (err, data) {
          if (err) {
            base.printStackTrace(err);
            reject(err);
          } else {
            resolve("Tmp Delete");
          }
        });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# afterTmpDeleteExecute : end"
      );
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  S3アクセスパラメータ初期化

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.afterMainExecute = function (
    args
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# afterMainExecute : start"
      );

      return new Promise(function (resolve, reject) {
        var s3params = base.getFirstIndexObject(args);
        base.writeLogTrace(
          "AbstractS3PutTriggerCommon# S3 delete param:" +
            JSON.stringify(s3params)
        );

        base.RequireObjects.S3.deleteObject(s3params, function (err, data) {
          if (err) {
            base.printStackTrace(err);
            reject(err);
          } else {
            base.writeLogInfo("AbstractS3PutTriggerCommon# S3 delete End");
            resolve(data);
          }
        });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractS3PutTriggerCommon# afterMainExecute : end");
    }
  }.bind(AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon);

  /*
  ワーカー処理（疑似スレッド処理）のクラスを返却（オーバーライド必須）
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getSubWorkerClazzFunc : start"
      );
      return require("./AbstractWorkerS3UploaderCommon.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractS3PutTriggerCommon# getSubWorkerClazzFunc : end"
      );
    }
  };

  /*
  順次処理する関数を指定する。
  Promiseを返却すると、Promiseの終了を待った上で順次処理をする
  
  前処理として、initEventParameterを追加

  @param event Lambdaの起動引数：event
  @param context Lambdaの起動引数：context
  */
  AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base = AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractS3PutTriggerCommon# getTasks :start");

      return [
        this.initS3PutTriggerParameter,
        this.waitExistsS3Object,
        this.beforeMainExecute,
        this.pdfParse,
        this.businessMainExecute,
        this.afterTmpDeleteExecute,
        this.afterMainExecute,
      ];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractS3PutTriggerCommon# getTasks :end");
    }
  };

  return {
    executeBizUnitCommon,
    AbstractS3PutTriggerCommon,
    AbstractBizCommon: AbstractS3PutTriggerCommon.prototype,
    AbstractBaseCommon: AbstractS3PutTriggerCommon.prototype.AbstractBaseCommon,
  };
};
