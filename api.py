from flask import Flask, jsonify, request

from spam_filter import spam

app = Flask(__name__)


@app.route("/api/find_spam", methods=["POST"])
def find_spam():
    payload = request.get_json()
    text_value = payload.get("text", None)

    if text_value is None:
        return jsonify({"success": False, "error": "Missing 'text' parameter"})

    try:
        is_spam = spam.is_spam(text_value)
        return jsonify({"success": True, "is_spam": is_spam})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    app.run(port=5000)
