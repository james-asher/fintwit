using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.IO;
using Tweetinvi;
using Tweetinvi.Events;
using Tweetinvi.Logic.JsonConverters;
using Tweetinvi.Models;
using Tweetinvi.Models.DTO;
using System.Threading;
using System.Security.Cryptography;

/// <summary>
/// 
/// This application contacts TWITTER and, based on inbound tweets, it generates banking transaction in JSON format.
/// 
/// Data can be output to a directory for storage and/or to console for interactive display.
/// 
/// Configuration files needed:
///     config.json - Defines how the application operates
///     unlock.key  - where you store the decryption key for sensitive key data
/// 
/// </summary>

namespace Gen1
{
    class Program
    {
        #region Global Variables
        static public Dictionary<string, long> accounts = new Dictionary<string, long>();
        static public long mybase = new DateTime(2017, 01, 01).Ticks;
        static public Random rnd = new Random();
        static public string statusInfo = "";
        static public bool consoleOutput = false;
        static public bool fileOutput = false;
        static public bool debugOutput = false;
        static public bool verboseOutput = false;
        static public int fileSet = 0;              //current file set number
        static public int fileCurrentCount = 0;     //current record count in file set

        // Global Configuration items:
        static public int baseAccounts = 500;
        static public int minStrength = 5000;
        static public bool multiFI = false;
        static public string fileLogLoc = "";
        static public string fileLogFilename = "";
        static public int fileCycle = 0;
        #endregion

        static void Main(string[] args)
        {
            #region Work Variables
            string keyFile = "";
            bool dataProtected = false;
            string unlockKey = "";
            string consumerKey = "";
            string consumerSecret = "";
            string accessToken = "";
            string accessTokenSecret = "";
            string[] tracks;
            #endregion

            #region Read and Setup Configuration data
            try
            {
                Dictionary<string, string> config = new Dictionary<string, string>();
                statusInfo = "Reading file config.json";
                config = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("config.json"));

                statusInfo = "Reading keyFile";
                keyFile = config["keyFile"];
                if (keyFile == "" || keyFile.ToUpper().Contains("NONE"))
                {
                    // No key file defined ... thus no security on the data
                    dataProtected = false;
                }
                else
                {
                    dataProtected = true;
                    statusInfo = "Reading file unlock.key";
                    unlockKey = File.ReadAllText(keyFile);
                    if (unlockKey =="" || unlockKey.ToUpper().Contains("NONE"))
                    {
                        dataProtected = false;
                    }
                }

                // Get the configuration items:
                statusInfo = "Reading consumerKey";
                consumerKey = config["consumerKey"];
                statusInfo = "Decrypting consumerKey";
                if (dataProtected) consumerKey = Encrypt.DecryptString(config["consumerKey"], unlockKey);
                statusInfo = "Reading consumerSecret";
                consumerSecret = config["consumerSecret"];
                statusInfo = "Decrypting consumerSecret";
                if (dataProtected) consumerSecret = Encrypt.DecryptString(config["consumerSecret"], unlockKey);
                statusInfo = "Reading accessToken";
                accessToken = config["accessToken"];
                statusInfo = "Decrypting accessToken";
                if (dataProtected) accessToken = Encrypt.DecryptString(config["accessToken"], unlockKey);
                statusInfo = "Reading accessTokenSecret";
                accessTokenSecret = config["accessTokenSecret"];
                statusInfo = "Decrypting accessTokenSecret";
                if (dataProtected) accessTokenSecret = Encrypt.DecryptString(config["accessTokenSecret"], unlockKey);
                unlockKey = null;
                statusInfo = "Reading tracks";
                tracks = config["tracks"].Split('|');
                statusInfo = "Reading baseAccounts";
                baseAccounts = int.Parse(config["baseAccounts"]);
                statusInfo = "Reading minStrength";
                minStrength = int.Parse(config["minStrength"]);
                statusInfo = "Reading multiFI";
                multiFI = config["multiFI"].ToUpper() == "TRUE";
                statusInfo = "Reading outputs.verbose";
                verboseOutput = config["outputs"].ToUpper().Contains("VERBOSE");
                statusInfo = "Reading outputs.debug";
                debugOutput = config["outputs"].ToUpper().Contains("DEBUG");
                statusInfo = "Reading outputs.console";
                consoleOutput = config["outputs"].ToUpper().Contains("CONSOLE");
                statusInfo = "Reading outputs.file";
                fileOutput = config["outputs"].ToUpper().Contains("FILE");
                if (fileOutput)
                {
                    statusInfo = "Reading fileLogLoc";
                    fileLogLoc = config["fileLogLoc"];
                    statusInfo = "Reading fileLogFilename";
                    fileLogFilename = config["fileLogFilename"];
                    statusInfo = "Reading fileCycle";
                    fileCycle = int.Parse(config["fileCycle"]);
                }
                statusInfo = "Finished reading config.json file.";

            }
            catch
            {
                Console.WriteLine("Bad Configuration.");
                Console.WriteLine("Status: " + statusInfo);
                Console.ReadLine();
                return;
            }
#endregion

            ITwitterCredentials creds = new TwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);

            Auth.SetCredentials(creds);
            if (debugOutput) Console.WriteLine("Credentials created");

            var stream = Tweetinvi.Stream.CreateFilteredStream();
            if (debugOutput) Console.WriteLine("Stream created");
            foreach (string track in tracks)
            {
                stream.AddTrack(track);
            }

            // Where to go when a tweet is received
            stream.MatchingTweetReceived += (sender, tweetAargs) =>
            {
                ReceivedTweet(tweetAargs);
            };

            if (debugOutput) Console.WriteLine("Stream Event Linked to worker");
            if (debugOutput) Console.WriteLine("Stream Starting");

            // Start listening for tweets
            stream.StartStreamMatchingAllConditions();

            if (debugOutput) Console.WriteLine("Streaming failed for some reason.  Trying to gracefully stop the Stream.");
            stream.StopStream();

            if (debugOutput) Console.WriteLine("Exiting...");
        }

        static public void ReceivedTweet(MatchedTweetReceivedEventArgs TweetReceived)
        {
            if (debugOutput) Console.Write("-");
            // Convert the tweet to an object and a string.
            string jTweet = TweetReceived.Json;
            var tweetDTO = JsonConvert.DeserializeObject<ITweetDTO>(jTweet, JsonPropertiesConverterRepository.Converters);
            string JTWEET = jTweet.ToUpper();

            // Source Account information
            string SrcAcctID = tweetDTO.CreatedBy.IdStr;
            string SrcCustName = tweetDTO.CreatedBy.ScreenName;

            // ### Calculate the transaction values.
            // Deposit value
            int SrcValue = rnd.Next(tweetDTO.CreatedBy.FollowersCount / 2, tweetDTO.CreatedBy.FollowersCount);
            // Xfer value
            int TrnValue = SrcValue / 3;

            // Destination Account information
            string DestAcctID = tweetDTO.InReplyToUserIdStr;
            string DestCustName = tweetDTO.InReplyToScreenName;

            // Test critiera collection
            bool BankIsFull = accounts.Count > baseAccounts;
            bool SrcInBank = (SrcAcctID == null) ? false : accounts.ContainsKey(SrcAcctID);
            bool DestInBank = (DestAcctID == null) ? false : accounts.ContainsKey(DestAcctID);
            string SrcBank = lastChar(SrcAcctID);
            string DestBank = lastChar(DestAcctID);
            bool SrcIsStrong = tweetDTO.CreatedBy.FollowersCount > minStrength;

            if (!BankIsFull & !SrcIsStrong)
            {
                // Bank is not full AND the Source Account is not strong enough = skip it
                return;
            }

            if (BankIsFull & !SrcInBank & !DestInBank)
            {
                // Bank is full AND Source Account is not in bank AND Destination Acccount is not in bank = skip it
                return;
            }


            // We have a good transaction to work on
            if (debugOutput) Console.WriteLine("");

            if (SrcInBank)
            {
                // Source Account already exists
                SrcBank = "0";
            }
            else
            {
                // Source Account needs to be created
                accounts.Add(SrcAcctID, SrcValue);
                SrcBank = "0";
                sendAcctAdd(SrcAcctID, SrcBank, SrcCustName, accounts.Count.ToString());
                sendDep(SrcAcctID, SrcBank, SrcValue.ToString());
            }



            if (DestAcctID != null)
            {
                if (accounts.ContainsKey(DestAcctID))
                {
                    // Destination Account already exists
                    DestBank = "0";
                    accounts[SrcAcctID] = accounts[SrcAcctID] - TrnValue;
                    accounts[DestAcctID] = accounts[DestAcctID] + TrnValue;
                    sendXfer(SrcAcctID, SrcBank, DestAcctID, DestBank, TrnValue.ToString());
                }
                else
                {
                    // Destination Account needs to be created
                    accounts.Add(DestAcctID, 0);
                    DestBank = "0";
                    accounts[SrcAcctID] = accounts[SrcAcctID] - TrnValue;
                    accounts[DestAcctID] = accounts[DestAcctID] + TrnValue;
                    sendAcctAdd(DestAcctID, DestBank, DestCustName, accounts.Count.ToString());
                    sendXfer(SrcAcctID, SrcBank, DestAcctID, DestBank, TrnValue.ToString());
                }

            }
            else if (SrcInBank)
            {
                // Source account exists and no destination account
                accounts[SrcAcctID] = accounts[SrcAcctID] + SrcValue;
                sendDep(SrcAcctID, SrcBank, SrcValue.ToString());
            }

            if (BankIsFull) if (debugOutput) Console.Write("#");
        }
        static public string lastChar(string invalue)
        {
            if (invalue == null) return "0";
            return invalue.Substring(invalue.Length - 1);
        }
        static public void sendAcctAdd(string acct, string fi, string name, string FISize)
        {
            DateTime now = DateTime.UtcNow;
            Dictionary<string, string> outdata = new Dictionary<string, string>();
            outdata.Add("trans", "acct");
            outdata.Add("fi", fi);
            outdata.Add("acct", acct);
            outdata.Add("name", name);
            outdata.Add("fisize", FISize);
            outdata.Add("seq", (now.Ticks - mybase).ToString());
            outdata.Add("date", now.ToString("o"));

            string jsonstring = JsonConvert.SerializeObject(outdata);

            output(jsonstring);
        }
        static public void sendDep(string acct, string fi, string amount)
        {
            DateTime now = DateTime.UtcNow;
            Dictionary<string, string> outdata = new Dictionary<string, string>();
            outdata.Add("trans", "deposit");
            outdata.Add("fi", fi);
            outdata.Add("acct", acct);
            outdata.Add("amt", amount);
            outdata.Add("seq", (now.Ticks - mybase).ToString());
            outdata.Add("date", now.ToString("o"));
            string jsonstring = JsonConvert.SerializeObject(outdata);

            output(jsonstring);

        }
        static public void sendXfer(string acct, string fi, string acct2, string fi2, string amount)
        {
            DateTime now = DateTime.UtcNow;
            Dictionary<string, string> outdata = new Dictionary<string, string>();
            outdata.Add("trans", "xfer");
            outdata.Add("fi", fi);
            outdata.Add("acct", acct);
            outdata.Add("fi2", fi2);
            outdata.Add("acct2", acct2);
            outdata.Add("amt", amount);
            outdata.Add("seq", (now.Ticks - mybase).ToString());
            outdata.Add("date", now.ToString("o"));

            string jsonstring = JsonConvert.SerializeObject(outdata);

            output(jsonstring);
        }
        static public void output(string jsonstring)
        {
            if (consoleOutput)
                Console.WriteLine(jsonstring);
            if (fileOutput)
            {
                fileCurrentCount++;
                if (fileCurrentCount > fileCycle)
                {
                    fileCurrentCount = 0;
                    fileSet++;
                }
                using (StreamWriter sw = File.AppendText(fileLogLoc + fileLogFilename + "_" + fileSet + ".log"))
                {
                    sw.WriteLine(jsonstring);
                }
            }

            //TODO:
            //
            //if (kafkaOutput)
            //{ }
            //if (redisOutput)
            //{ }
        }
    }

    // Not my code
    public static class Encrypt
    {
        // This size of the IV (in bytes) must = (keysize / 8).  Default keysize is 256, so the IV must be
        // 32 bytes long.  Using a 16 character string here gives us 32 bytes when converted to a byte array.
        private const string initVector = "getYourOwnCode!!";
        // This constant is used to determine the keysize of the encryption algorithm
        private const int keysize = 256;
        //Encrypt
        public static string EncryptString(string plainText, string passPhrase)
        {
            byte[] initVectorBytes = Encoding.UTF8.GetBytes(initVector);
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            PasswordDeriveBytes password = new PasswordDeriveBytes(passPhrase, null);
            byte[] keyBytes = password.GetBytes(keysize / 8);
            RijndaelManaged symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform encryptor = symmetricKey.CreateEncryptor(keyBytes, initVectorBytes);
            MemoryStream memoryStream = new MemoryStream();
            CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write);
            cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);
            cryptoStream.FlushFinalBlock();
            byte[] cipherTextBytes = memoryStream.ToArray();
            memoryStream.Close();
            cryptoStream.Close();
            return Convert.ToBase64String(cipherTextBytes);
        }
        //Decrypt
        public static string DecryptString(string cipherText, string passPhrase)
        {
            byte[] initVectorBytes = Encoding.UTF8.GetBytes(initVector);
            byte[] cipherTextBytes = Convert.FromBase64String(cipherText);
            PasswordDeriveBytes password = new PasswordDeriveBytes(passPhrase, null);
            byte[] keyBytes = password.GetBytes(keysize / 8);
            RijndaelManaged symmetricKey = new RijndaelManaged();
            symmetricKey.Mode = CipherMode.CBC;
            ICryptoTransform decryptor = symmetricKey.CreateDecryptor(keyBytes, initVectorBytes);
            MemoryStream memoryStream = new MemoryStream(cipherTextBytes);
            CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
            byte[] plainTextBytes = new byte[cipherTextBytes.Length];
            int decryptedByteCount = cryptoStream.Read(plainTextBytes, 0, plainTextBytes.Length);
            memoryStream.Close();
            cryptoStream.Close();
            return Encoding.UTF8.GetString(plainTextBytes, 0, decryptedByteCount);
        }
    }
}
