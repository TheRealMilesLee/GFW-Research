#!/bin/bash
websites_ipv6=(
  "ipv6.google.com"
  "ipv6.test-ipv6.com"
  "www.kame.net"
  "ipv6.he.net"
  "ipv6.eurov6.org"
  "www.sixxs.net"
  "ipv6.ripe.net"
  "www.isc.org"
  "ipv6.facebook.com"
  "ipv6.bbc.co.uk"
  "ipv6.microsite.prod.nic.ad.jp"
  "www.v6.facebook.com"
  "ipv6.youtube.com"
  "ipv6.twitter.com"
  "ipv6.reddit.com"
  "ipv6.xnxx.com"
  "ipv6.netflix.com"
  "ipv6.apnic.net"
  "ipv6.baidu.com"
  "ipv6.sina.com.cn"
  "ipv6.taobao.com"
  "ipv6.juniper.net"
  "ipv6.cisco.com"
  "ipv6.yandex.com"
  "ipv6.wipo.int"
  "ipv6.cloudflare.com"
  "ipv6.amazon.com"
  "ipv6.apple.com"
  "ipv6.ibm.com"
  "ipv6.microsoft.com"
)

websites_gfw_ipv6=(
  "www.reddit.com"
  "www.twitter.com"
  "www.facebook.com"
  "www.instagram.com"
  "www.google.com"
  "www.youtube.com"
  "www.dropbox.com"
  "www.wikipedia.org"
  "www.linkedin.com"
  "www.whatsapp.com"
  "www.telegram.org"
  "www.medium.com"
  "www.blogspot.com"
  "www.wordpress.com"
  "www.vimeo.com"
  "www.tumblr.com"
  "www.slack.com"
  "www.github.com"
  "www.netflix.com"
  "www.flickr.com"
  "www.soundcloud.com"
  "www.quora.com"
  "www.pinterest.com"
  "www.snapchat.com"
  "www.twitch.tv"
  "www.periscope.tv"
  "www.disqus.com"
  "www.dailymotion.com"
  "www.meetup.com"
  "www.tiktok.com"
)

mainstream_social_media=(
  "www.facebook.com"
  "www.twitter.com"
  "www.instagram.com"
  "www.linkedin.com"
  "www.tiktok.com"
  "www.snapchat.com"
  "www.reddit.com"
  "www.pinterest.com"
  "www.tumblr.com"
  "www.quora.com"
  "www.medium.com"
  "www.whatsapp.com"
  "www.telegram.org"
  "www.wechat.com"
  "www.youtube.com"
  "www.vimeo.com"
  "www.dailymotion.com"
  "www.twitch.tv"
  "www.discord.com"
  "www.vk.com"
  "www.line.me"
  "www.weibo.com"
  "www.douyin.com"
  "www.kuaishou.com"
  "www.qq.com"
  "www.signal.org"
  "www.clubhouse.com"
  "www.meetup.com"
  "www.viber.com"
  "www.periscope.tv"
)

function test_connectivity {
  local website=$1
  echo "Testing $website..."

  if curl -4 -s --head --request GET "$website" | grep "200 OK" >/dev/null; then
    echo "IPv4: OK"
  else
    echo "IPv4: Failed"
  fi

  if curl -6 -s --head --request GET "$website" | grep "200 OK" >/dev/null; then
    echo "IPv6: OK"
  else
    echo "IPv6: Failed"
  fi

  echo ""
}
for website in "${websites_ipv6[@]}"; do
  test_connectivity "$website"
done

for website in "${websites_gfw_ipv6[@]}"; do
  test_connectivity "$website"
done
for website in "${mainstream_social_media[@]}"; do
  test_connectivity "$website"
done
