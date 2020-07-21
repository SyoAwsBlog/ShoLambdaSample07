module.exports = function AbstractWorkerS3UploaderCommon() {
  var Promise;

  // 継承：親クラス＝AbstractBizCommon.js
  var superClazzFunc = require("./AbstractBizCommon.js");
  AbstractWorkerS3UploaderCommon.prototype = new superClazzFunc();

  // 各ログレベルの宣言
  var LOG_LEVEL_TRACE = 1;
  var LOG_LEVEL_DEBUG = 2;
  var LOG_LEVEL_INFO = 3;
  var LOG_LEVEL_WARN = 4;
  var LOG_LEVEL_ERROR = 5;

  // 現在の出力レベルを設定(ワーカー処理は別のログレベルを設定可能とする)
  var LOG_LEVEL_CURRENT = LOG_LEVEL_INFO;
  if (process && process.env && process.env.LogLevelForWorker) {
    LOG_LEVEL_CURRENT = process.env.LogLevelForWorker;
  }

  // 出力用S3のBucket名
  var OUTPUT_S3_BUCKET_NAME = "";
  if (process && process.env && process.env.OutputS3BucketName) {
    OUTPUT_S3_BUCKET_NAME = process.env.OutputS3BucketName;
  }

  /*
  現在のログレベルを返却する
  ※　処理制御側とログレベルを同一設定で行う場合は、オーバーライドをコメントアウトする
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.getLogLevelCurrent = function () {
    return LOG_LEVEL_CURRENT;
  }.bind(AbstractWorkerS3UploaderCommon.prototype);
  /*
  出力レベル毎のログ処理
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.writeLogTrace = function (
    msg
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelTrace() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype);

  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.writeLogDebug = function (
    msg
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelDebug() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype);

  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.writeLogInfo = function (
    msg
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelInfo() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype);

  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.writeLogWarn = function (
    msg
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelWarn() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype);

  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.writeLogError = function (
    msg
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    if (base.getLogLevelError() >= base.getLogLevelCurrent()) {
      console.log(msg);
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype);

  /*
  出力用S3のBucket名を返却する
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.getOutputS3BucketName = function () {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# getOutputS3BucketName : start"
      );

      return OUTPUT_S3_BUCKET_NAME;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# getOutputS3BucketName : end"
      );
    }
  }.bind(AbstractWorkerS3UploaderCommon);

  // 処理の実行
  function* executeBizWorkerCommon(event, context, bizRequireObjects) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# executeBizWorkerCommon : start"
      );
      if (bizRequireObjects.PromiseObject) {
        Promise = bizRequireObjects.PromiseObject;
      }
      AbstractWorkerS3UploaderCommon.prototype.RequireObjects = bizRequireObjects;

      return yield AbstractWorkerS3UploaderCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# executeBizWorkerCommon : end"
      );
    }
  }
  AbstractWorkerS3UploaderCommon.prototype.executeBizWorkerCommon = executeBizWorkerCommon;

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# beforeMainExecute : start"
      );

      base.writeLogInfo(
        "AbstractWorkerS3UploaderCommon# beforeMainExecute:args:" +
          JSON.stringify(args)
      );

      return Promise.resolve(args);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# beforeMainExecute : end"
      );
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon);

  /*
  業務前処理

  @param args 実行結果配列（最初の処理は、Lambdaの起動引数：record)
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# businessMainExecute : start"
      );

      var targetInfos = base.getFirstIndexObject(args);

      var pdfReader = targetInfos.pdfReader;
      var baseKeyName = targetInfos.baseKeyName;
      var pageNo = targetInfos.pageNo;

      var MemoryStream = base.RequireObjects.MemoryStream;
      var memoryWriter = new MemoryStream.WritableStream();

      var Hummus = base.RequireObjects.Hummus;
      var pdfWriter = Hummus.createWriter(
        new Hummus.PDFStreamForResponse(memoryWriter)
      );

      var outputS3params = {};
      outputS3params.Bucket = base.getOutputS3BucketName();
      outputS3params.Key = baseKeyName + String(pageNo) + ".pdf";

      pdfWriter.createPDFCopyingContext(pdfReader).appendPDFPageFromPDF(pageNo);
      pdfWriter.end();
      outputS3params.Body = memoryWriter.toBuffer();

      return new Promise(function (resolve, reject) {
        base.RequireObjects.S3.upload(outputS3params, function (err, data) {
          if (err) {
            base.printStackTrace(err);
            reject(err);
          } else {
            resolve(data);
          }
        });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractWorkerS3UploaderCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon);

  /*
  順次処理する関数を指定する。
  Promiseを返却すると、Promiseの終了を待った上で順次処理をする
  
  前処理として、initEventParameterを追加

  @param event Lambdaの起動引数：event
  @param context Lambdaの起動引数：context
  */
  AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon.getTasks = function (
    event,
    context
  ) {
    var base = AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("AbstractWorkerS3UploaderCommon# getTasks :start");

      return [this.beforeMainExecute, this.businessMainExecute];
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("AbstractWorkerS3UploaderCommon# getTasks :end");
    }
  };

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    executeBizWorkerCommon,
    AbstractBaseCommon:
      AbstractWorkerS3UploaderCommon.prototype.AbstractBaseCommon,
    AbstractBizCommon: AbstractWorkerS3UploaderCommon.prototype,
  };
};
