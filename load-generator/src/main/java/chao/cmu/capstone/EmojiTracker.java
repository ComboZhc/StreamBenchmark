package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiManager;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Print Emoji aliases
 */
public class EmojiTracker {

    public static List<String> getEmojiAliases(String str) {
        List<String> aliases = new ArrayList<>();
        Collection<Emoji> emojis = EmojiManager.getAll();
        for (Emoji emoji : emojis) {
            int pos = str.indexOf(emoji.getUnicode());
            while (pos >= 0) {
                aliases.add(emoji.getAliases().get(0));
                pos = str.indexOf(emoji.getUnicode(), pos + emoji.getUnicode().length());
            }
        }
        return aliases;
    }

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();
        while (true) {
            String msg = queue.take();
            if (Utils.isTweet(msg)) {
                try {
                    JSONObject obj = new JSONObject(msg);
                    String text = (String)obj.get("text");
                    List<String> aliases = getEmojiAliases(text);
                    for (String alias : aliases)
                        System.out.println(alias);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
