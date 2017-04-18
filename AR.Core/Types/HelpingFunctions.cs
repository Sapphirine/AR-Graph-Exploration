using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Types
{
    public static class HelpingFunctions
    {

        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        /// <summary>
        /// Unity HTTP Get Method
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        public static WWW Get(string url)
        {

            WWW www = new WWW(url);

            WaitForSeconds w;
            while (!www.isDone)
                w = new WaitForSeconds(0.1f);

            return www;
        }


        /// <summary>
        /// Unity HTTP post Method with return
        /// </summary>
        /// <param name="url"></param>
        /// <param name="post"></param>
        /// <returns></returns>
        public static WWW Post(string url, Dictionary<string, string> post)
        {
            WWWForm form = new WWWForm();
            foreach (var pair in post)
                form.AddField(pair.Key, pair.Value);

            WWW www = new WWW(url, form);

            WaitForSeconds w;
            while (!www.isDone)
                w = new WaitForSeconds(0.1f);

            return www;
        }


    }
}
