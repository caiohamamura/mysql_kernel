# i18n.py
import gettext
import locale 
import os

def get_translator(lang=None):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    locale_dir = os.path.join(base_dir, 'locale')
    if lang is None:
        lang = locale.getdefaultlocale()[0]
    if gettext.find('messages', localedir=locale_dir, languages=[lang]) is None:
        lang = lang[0:2]

    return gettext.translation(
        'messages',
        localedir=locale_dir,
        languages=[lang],
        fallback=True
    ).gettext

def has_translation(lang=None):
    lang = locale.getdefaultlocale()[0]
    try:
        get_translator(lang)
        return f"Tem tradução {lang}"
    except:
        return "False"