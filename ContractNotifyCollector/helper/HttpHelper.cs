using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ContractNotifyCollector.helper
{
    class HttpHelper
    {
        public static string Post(string url, string method, JArray _params)
        {
            var json = new JObject() {
                    { "id", 1 },
                    { "jsonrpc", "2.0" },
                    { "method", method },
                    { "params", _params },
                }.ToString();

            var Res = HttpPost(url, Encoding.UTF8.GetBytes(json.ToString()));
            return Res.Result;
        }
        public static async Task<string> HttpPost(string url, byte[] data)
        {
            WebClient wc = new WebClient();
            wc.Headers["content-type"] = "text/plain;charset=UTF-8";
            byte[] retdata = await wc.UploadDataTaskAsync(url, "POST", data);
            return Encoding.UTF8.GetString(retdata);
        }


        public static string Get(string url)
        {
            var Res = HttpGet(url);
            return Res.Result;
        }
        public static async Task<string> HttpGet(string url)
        {
            WebClient wc = new WebClient();
            wc.Headers["content-type"] = "text/plain;charset=UTF-8";
            byte[] retdata = await wc.DownloadDataTaskAsync(url);
            return Encoding.UTF8.GetString(retdata);
        }
    }
}
