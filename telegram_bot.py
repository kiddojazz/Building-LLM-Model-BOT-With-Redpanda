# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 01:20:04 2024

@author: olanr
"""

telegram_token = "7772898402:AAEa3JnJ8IidENDv_MStiMKL9Cx3Q2RZJPo"


import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from llm_call import ask_question

# Initialize the bot
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! I'm your logging bot, ready to help you!")

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This handler echoes back any message sent
    user_message = update.message.text
    # Process the user's message
    response = ask_question(user_message)
    await update.message.reply_text(response)


if __name__ == '__main__':
    # Set up logging for debugging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Replace 'YOUR_TELEGRAM_TOKEN' with the token you received from BotFather
    application = ApplicationBuilder().token(telegram_token).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    # Run the bot until you press Ctrl+C
    application.run_polling()
