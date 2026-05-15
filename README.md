[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/4r5TpSOc)
# Zadatak 6: Napredno upravljanje događajima i obrada grešaka pomoću Apache Kafka brokera

## Kontekst
U okviru postojećeg repozitorijuma definisana je osnovna mikroservisna arhitektura koja se sastoji od servisa za narudžbine (`orders-service`), servisa za proizvode (`products-service`), `API gateway` i servisa za notifikacije (`notifications-service`). Komunikacija između mikroservisa se obavlja asinhrono, posredstvom Apache Kafka brokera. 

Potrebno je proširiti postojeći sistem implementacijom mehanizama za obradu specifičnih poslovnih grešaka kroz arhitekturu zasnovanu na događajima (Event-Driven Architecture).

## Specifikacija zadatka

Vaš zadatak je da dogradite logiku servisa kako bi sistem adekvatno reagovao na izuzetne situacije prilikom procesiranja narudžbine, te da te situacije komunicirate ka korisniku putem sistema za obaveštavanje. 

Potrebno je implementirati detekciju i asinhrono slanje poruka za sledeća dva scenarija:

### 1. Pokušaj naručivanja nepostojećeg proizvoda
Kada `orders-service` inicira proces narudžbine i pošalje događaj, `products-service` vrši proveru raspoloživosti. Ukoliko traženi proizvod ne postoji u bazi, `products-service` je u obavezi da generiše događaj o neuspehu i publikuje ga na novi Kafka topic posvećen ovoj vrsti greške (npr. `product_not_found_events`).

### 2. Nedovoljna količina proizvoda na stanju
Ukoliko traženi proizvod postoji, ali je njegova raspoloživa količina na stanju manja od zahtevane (nije moguće izvršiti rezervaciju), `products-service` mora da detektuje ovaj konflikt i publikuje odgovarajući događaj na zaseban Kafka topic namenjen problemima sa zalihama (npr. `out_of_stock_events`).

### 3. Proširenje Servisa za notifikacije (`notifications-service`)
*   **(Consumers):** U okviru `notifications-service` servisa potrebno je implementirati Kafka Consumer-e koji će osluškivati novoformirane topic-e (`product_not_found_events` i `out_of_stock_events`).
*   **Obrada događaja:** Po prijemu poruke sa ovih topic-a, servis za notifikacije treba da parsira događaj i simulira slanje obaveštenja klijentu sa formatiranom porukom koja sadrži ID narudžbine i jasan razlog odbijanja.

### 4. Struktura poruka
Sve Kafka poruke za greške moraju biti serijalizovane u JSON formatu. Očekivana struktura poruke (payload) mora sadržati minimum sledeća polja:
*   `order_id`: Jedinstveni identifikator narudžbine koja je odbijena.
*   `product_id`: Identifikator problematičnog proizvoda.
*   `timestamp`: Vreme nastanka događaja.
*   `error_reason`: Tekstualni opis greške (npr. "Proizvod ne postoji u katalogu" ili "Nedovoljna količina na stanju").
