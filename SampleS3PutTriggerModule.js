/*
処理固有に必要な処理などを、この層に実装する。

テストや挙動確認を含めたコードをコメントアウト込みで、
サンプルとして記述する。

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function SampleS3PutTriggerModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = new require("./AbstractS3PutTriggerCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleS3PutTriggerModule.prototype = new superClazzFunc();

  // S3のオブジェクト確認待ちリトライ上限
  var OBJECT_WAIT_FOR_RETRY_MAX = 3;
  if (process && process.env && process.env.ObjectWaitForRetryMax) {
    OBJECT_WAIT_FOR_RETRY_MAX = process.env.ObjectWaitForRetryMax;
  }

  // 処理の実行
  function* execute(event, context, bizRequireObjects) {
    var base = SampleS3PutTriggerModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleS3PutTriggerModule# execute : start");

      // 親の業務処理を実行
      return yield SampleS3PutTriggerModule.prototype.executeBizUnitCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleS3PutTriggerModule# execute : end");
    }
  }

  SampleS3PutTriggerModule.prototype.AbstractBaseCommon.getObjectWaitForRetryMax = function () {
    var base = SampleS3PutTriggerModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleS3PutTriggerModule# getObjectWaitForRetryMax : start"
      );
      return OBJECT_WAIT_FOR_RETRY_MAX;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleS3PutTriggerModule# getObjectWaitForRetryMax : end"
      );
    }
  }.bind(SampleS3PutTriggerModule);

  /*
  ワーカー処理（疑似スレッド処理）用のクラスを返却
  */
  SampleS3PutTriggerModule.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = SampleS3PutTriggerModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "SampleS3PutTriggerModule# getSubWorkerClazzFunc : start"
      );
      return require("./SampleWorkerS3UploaderModule.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "SampleS3PutTriggerModule# getSubWorkerClazzFunc : end"
      );
    }
  };

  return {
    execute,
  };
};
