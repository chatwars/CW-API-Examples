const { ConsumerGroup } = require('kafka-node')
const { Telegraf, Extra } = require('telegraf')

const targetChatId = -1001391051921 // place your Channel ID
let dealsBuffer = []

// Telegram bot
const bot = new Telegraf(process.env.BOT_TOKEN)
bot.launch()

setInterval(() => {
  const groupedDeals = partitionByItem(dealsBuffer)
  dealsBuffer = []
  if (groupedDeals.size === 0) return
  const text = formatMessageBody(groupedDeals)
  bot.telegram.sendMessage(targetChatId, text, Extra.HTML()).catch(console.error)
}, 10000)

// CW API
const options = {
  kafkaHost: 'digest-api.chtwrs.com:9092',
  groupId: 'my-group', // rename to your random
  fromOffset: 'earliest',
  commitOffsetsOnFirstJoin: true,
  autoCommit: true
}
const consumer = new ConsumerGroup(options, 'cw3-deals')

consumer.on('message', message => {
  const deal = JSON.parse(message.value)
  dealsBuffer.push(deal)
})

// Logic
function partitionByItem (deals) {
  const groups = new Map()
  deals.forEach(deal => {
    if (groups.has(deal.item)) {
      groups.get(deal.item).push(deal)
    } else {
      groups.set(deal.item, [deal])
    }
  })
  return groups
}

function formatMessageBody (groupedDeals) {
  let msgBody = '<pre>'
  groupedDeals.forEach((deals, itemName) => {
    msgBody += `${itemName}:\n` + deals.map(formatDealLine).join('\n') + '\n'
  })
  msgBody += '</pre>'
  return msgBody
}

function formatDealLine (deal) {
  const { sellerName, sellerCastle, buyerName, buyerCastle, qty, price } = deal
  return `${sellerCastle}${sellerName} => ${buyerCastle}${buyerName}, ${qty} x ${price}💰`
}
