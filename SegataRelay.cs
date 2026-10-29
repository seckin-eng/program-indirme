// SEGATA Security - Sohbet relay sunucusu (Windows, .NET Framework 4.x - ek kurulum gerektirmez).
// Mesajları CANLI iletir; sunucuda KALICI SAKLAMAZ. Alıcı çevrimdışıysa kısa süre bellekte tutar.
// Protokol: her satır bir JSON nesnesi (newline ile), UTF-8 — Python relay ile birebir aynı.
//   İstemci -> Sunucu: {"type":"hello","id":"<telefon>"} | {"type":"msg","to":..,"from":..,"ts":..,"mid":..,"body":..} | {"type":"ping"} | {"type":"who","id":..}
//   Sunucu -> İstemci: {"type":"welcome"} | gelen msg | {"type":"ack","mid":..,"status":"delivered|queued"} | {"type":"presence","id":..,"online":..} | {"type":"pong"}
// Kullanım: SegataRelay.exe [port] [host]   (varsayılan 9696 0.0.0.0)
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace SegataRelay
{
    static class Program
    {
        const int OfflineMax = 200;
        static readonly TimeSpan OfflineTtl = TimeSpan.FromDays(7);

        static readonly Dictionary<string, TcpClient> clients = new Dictionary<string, TcpClient>();
        class Queued { public DateTime Exp; public string Line; }
        static readonly Dictionary<string, List<Queued>> offline = new Dictionary<string, List<Queued>>();
        static readonly object gate = new object();

        static void Main(string[] args)
        {
            int port = 9696;
            if (args.Length > 0) int.TryParse(args[0], out port);
            IPAddress host = IPAddress.Any;
            if (args.Length > 1) IPAddress.TryParse(args[1], out host);

            var listener = new TcpListener(host, port);
            listener.Start(200);
            Console.WriteLine("SEGATA relay dinliyor: " + host + ":" + port);

            var reaper = new Thread(Reaper); reaper.IsBackground = true; reaper.Start();

            while (true)
            {
                TcpClient c = listener.AcceptTcpClient();
                var t = new Thread(() => Handle(c)); t.IsBackground = true; t.Start();
            }
        }

        static string Digits(string s)
        {
            if (s == null) return "";
            var sb = new StringBuilder();
            foreach (char ch in s) if (ch >= '0' && ch <= '9') sb.Append(ch);
            return sb.ToString();
        }

        static bool SendLine(TcpClient c, string json)
        {
            try
            {
                byte[] d = Encoding.UTF8.GetBytes(json + "\n");
                var ns = c.GetStream();
                lock (c) { ns.Write(d, 0, d.Length); ns.Flush(); }
                return true;
            }
            catch { return false; }
        }

        static string DeliverOrQueue(string to, string line)
        {
            TcpClient c;
            lock (gate) { clients.TryGetValue(to, out c); }
            if (c != null && SendLine(c, line)) return "delivered";
            lock (gate)
            {
                List<Queued> q;
                if (!offline.TryGetValue(to, out q)) { q = new List<Queued>(); offline[to] = q; }
                q.Add(new Queued { Exp = DateTime.UtcNow + OfflineTtl, Line = line });
                if (q.Count > OfflineMax) q.RemoveRange(0, q.Count - OfflineMax);
            }
            return "queued";
        }

        static void FlushOffline(string phone, TcpClient c)
        {
            List<Queued> q;
            lock (gate) { offline.TryGetValue(phone, out q); offline.Remove(phone); }
            if (q == null) return;
            DateTime now = DateTime.UtcNow;
            foreach (var item in q) if (item.Exp >= now) SendLine(c, item.Line);
        }

        static void BroadcastPresence(string phone, bool online)
        {
            string note = "{\"type\":\"presence\",\"id\":\"" + phone + "\",\"online\":" + (online ? "true" : "false") + "}";
            List<TcpClient> conns;
            lock (gate) { conns = new List<TcpClient>(clients.Values); }
            foreach (var c in conns) SendLine(c, note);
        }

        static void Handle(TcpClient conn)
        {
            string phone = null;
            var buf = new StringBuilder();
            try
            {
                conn.ReceiveTimeout = 125000;
                var ns = conn.GetStream();
                byte[] b = new byte[8192];
                while (true)
                {
                    int n;
                    try { n = ns.Read(b, 0, b.Length); }
                    catch (System.IO.IOException) { if (!SendLine(conn, "{\"type\":\"ping\"}")) break; continue; }
                    if (n <= 0) break;
                    buf.Append(Encoding.UTF8.GetString(b, 0, n));
                    int idx;
                    while ((idx = IndexOf(buf, '\n')) >= 0)
                    {
                        string line = buf.ToString(0, idx).Trim();
                        buf.Remove(0, idx + 1);
                        if (line.Length == 0) continue;
                        string type = Field(line, "type");
                        if (type == "hello")
                        {
                            phone = Digits(Field(line, "id"));
                            if (phone.Length == 0) continue;
                            TcpClient old = null;
                            lock (gate) { clients.TryGetValue(phone, out old); clients[phone] = conn; }
                            if (old != null && old != conn) { try { old.Close(); } catch { } }
                            SendLine(conn, "{\"type\":\"welcome\"}");
                            FlushOffline(phone, conn);
                            BroadcastPresence(phone, true);
                        }
                        else if (type == "msg")
                        {
                            string to = Digits(Field(line, "to"));
                            if (to.Length == 0) continue;
                            string status = DeliverOrQueue(to, line);
                            string mid = Field(line, "mid");
                            if (mid.Length > 0) SendLine(conn, "{\"type\":\"ack\",\"mid\":\"" + mid + "\",\"status\":\"" + status + "\"}");
                        }
                        else if (type == "ping") SendLine(conn, "{\"type\":\"pong\"}");
                        else if (type == "who")
                        {
                            string to = Digits(Field(line, "id"));
                            bool on; lock (gate) { on = clients.ContainsKey(to); }
                            SendLine(conn, "{\"type\":\"presence\",\"id\":\"" + to + "\",\"online\":" + (on ? "true" : "false") + "}");
                        }
                    }
                }
            }
            catch { }
            finally
            {
                if (phone != null)
                {
                    bool removed = false;
                    lock (gate) { TcpClient cur; if (clients.TryGetValue(phone, out cur) && cur == conn) { clients.Remove(phone); removed = true; } }
                    if (removed) BroadcastPresence(phone, false);
                }
                try { conn.Close(); } catch { }
            }
        }

        static int IndexOf(StringBuilder sb, char c)
        {
            for (int i = 0; i < sb.Length; i++) if (sb[i] == c) return i;
            return -1;
        }

        // Düz JSON nesnesinden tek bir alanın değerini çıkarır (string veya sayı).
        static string Field(string json, string key)
        {
            string pat = "\"" + key + "\"";
            int k = json.IndexOf(pat, StringComparison.Ordinal);
            if (k < 0) return "";
            int i = k + pat.Length;
            while (i < json.Length && (json[i] == ' ' || json[i] == ':')) i++;
            if (i >= json.Length) return "";
            if (json[i] == '"')
            {
                i++;
                var sb = new StringBuilder();
                while (i < json.Length)
                {
                    char ch = json[i++];
                    if (ch == '\\' && i < json.Length) { sb.Append(json[i++]); }
                    else if (ch == '"') break;
                    else sb.Append(ch);
                }
                return sb.ToString();
            }
            else
            {
                int st = i;
                while (i < json.Length && json[i] != ',' && json[i] != '}') i++;
                return json.Substring(st, i - st).Trim();
            }
        }

        static void Reaper()
        {
            while (true)
            {
                Thread.Sleep(3600000);
                DateTime now = DateTime.UtcNow;
                lock (gate)
                {
                    var keys = new List<string>(offline.Keys);
                    foreach (var p in keys)
                    {
                        var q = offline[p];
                        q.RemoveAll(x => x.Exp < now);
                        if (q.Count == 0) offline.Remove(p);
                    }
                }
            }
        }
    }
}
