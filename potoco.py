from flask import Flask, send_file


app = Flask(__name__)

@app.route('/potoco', methods=['GET'])
def get_dados_json():
    # Caminho para o arquivo JSON (coloque o caminho correto)
    arquivo_json = 'data.json'

    # Usamos o send_file para retornar o arquivo JSON
    return send_file(arquivo_json, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)