## TP3 – Dashboard Interactif avec Streamlit + DuckDB

### Objectifs

- Visualisation des données traitées
- Interaction utilisateur avec requêtes en direct

### Travaux

1. Reprendre les données précédemment ingérées dans DuckDB.
2. Créer une app Streamlit dans un container à part qui :
   - Connecte à DuckDB (via volume partagé ou fichier monté)
   - Permet de sélectionner des filtres (dates, régions, etc.)
   - Affiche des KPI, des graphiques (Bar, Line, Map si géo)
3. Dockeriser l’application Streamlit.

- Ajouter une barre de recherche (requêtes dynamiques)
- Proposer des exports CSV des vues actuelles