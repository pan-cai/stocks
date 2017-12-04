# _*_ coding:utf-8 _*_

from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World! 这是我修改的'


@app.route('/hehe/')
def hello_world():
    return 'Hello World! 这是我修改的2'


if __name__ == '__main__':
    app.run(debug=True)
