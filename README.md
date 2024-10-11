# Projet-Elasticsearch-IPSSI

## Pré-requis

Avant de commencer, assurez-vous d'avoir installé les éléments suivants sur votre système :

- Docker
- Docker Compose
- Python
- pip

## Comment lancer le projet

Suivez ces étapes pour mettre en place et exécuter le projet :

### 1. Cloner le projet :

```bash
git clone https://github.com/AxelSELEBRANTSF/Projet-Elasticsearch-IPSSI.git
```

### 2. Accéder au répertoire du projet :

```bash
cd Projet-Elasticsearch-IPSSI
```

### 3. Installer les dépendances :

```bash
pip install -r requirements.txt
```

### 4. Lancer Docker Compose :

```bash
docker compose up --build
```

### 5. Lancer un container MongoDB :

```bash
docker run -d --name mongodb mongo
```

### 6. Lancer le script __init__.py :

```bash
python __init__.py
```

### 7. Accéder à l'interface Kibana :

Ouvrez votre navigateur et allez à l'adresse http://localhost:5601
