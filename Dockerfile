
FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Evita arquivos .pyc e mantém logs no terminal
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copia as dependências
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia os componentes e dados
COPY components/ ./components/
COPY data/ ./data/

# Cria diretório para os resultados
RUN mkdir -p /app/data/output_s2

# Executa o componente S2
CMD ["python", "-m", "components.S2.main"]
