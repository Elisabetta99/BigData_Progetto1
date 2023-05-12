# Progetto Big Data
 Progetto realizzato per il corso di Big Data da Elisabetta Giordano e Giulia Castagnacci.
 
 Specifiche dei Job:
 * **Job 1**: generare, per ciascun anno, i 10 prodotti che hanno ricevuto il maggior numero di recensioni e, per ciascuno di essi, le 5 parole con almeno 4 caratteri più frequentemente usate nelle recensioni (campo text), indicando, per ogni parola, il numero di occorrenze della parola.
 * **Job 2**: generare una lista di utenti ordinata sulla base del loro apprezzamento, dove l’apprezzamento di ogni utente è ottenuto dalla media dell’utilità (rapporto tra HelpfulnessNumerator e HelpfulnessDenominator) delle recensioni che hanno scritto, indicando per ogni utente il loro apprezzamento.
 * **Job 3**: generare gruppi di utenti con gusti affini, dove gli utenti hanno gusti affini se hanno recensito con score superiore o uguale a 4 almeno 3 prodotti in comune, indicando, per ciascun gruppo, i prodotti condivisi. Il risultato deve essere ordinato in base allo UserId del primo elemento del gruppo e non devono essere presenti duplicati.

Ciascun Job è stato progettato e realizzato in *MapReduce*, *Hive*, *Spark*.

Dataset utilizzato: [Amazon Fine Food Reviews](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews)
