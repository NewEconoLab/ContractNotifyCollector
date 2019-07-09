using Newtonsoft.Json.Linq;
using ThinNeo;

namespace ContractNotifyCollector.helper
{
    class ScriptHelper
    {
        public static string InvokeScript(string url, string method, string schash, string scmethod, params string[] scparams)
        {
            byte[] data = null;
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                MyJson.JsonNode_Array array = new MyJson.JsonNode_Array();

                for (var i = 0; i < scparams.Length; i++)
                {
                    array.AddArrayValue(scparams[i]);
                }
                sb.EmitParamJson(array);
                sb.EmitPushString(scmethod);
                sb.EmitAppCall(new Hash160(schash));
                data = sb.ToArray();
            }
            string script = ThinNeo.Helper.Bytes2HexString(data);
            var res = HttpHelper.Post(url, method, new JArray { new JValue(script) });
            return res;
        }
    }
}
