module.exports = function SampleWorkerS3UploaderModule() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractWorkerS3UploaderCommon.js");
  // prototypeにセットする事で継承関係のように挙動させる
  SampleWorkerS3UploaderModule.prototype = new superClazzFunc();

  function* execute(event, context, RequireObjects) {
    var base = SampleWorkerS3UploaderModule.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace("SampleWorkerS3UploaderModule# execute : start");

      return yield SampleWorkerS3UploaderModule.prototype.executeBizWorkerCommon(
        event,
        context,
        RequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace("SampleWorkerS3UploaderModule# execute : end");
    }
  }
  SampleWorkerS3UploaderModule.prototype.execute = execute;

  // 関数定義は　return　より上部に記述
  // 外部から実行できる関数をreturnすること
  return {
    execute,
    SampleWorkerS3UploaderModule,
    AbstractBaseCommon:
      SampleWorkerS3UploaderModule.prototype.AbstractBaseCommon,
    AbstractBizCommon:
      SampleWorkerS3UploaderModule.prototype.AbstractBizCommon,
    AbstractWorkerChildCommon: SampleWorkerS3UploaderModule.prototype,
  };
};
