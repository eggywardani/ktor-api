package com.eggy.userapi

import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.http.*
import io.ktor.gson.*
import io.ktor.features.*
import io.ktor.request.receive
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.jodatime.date
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    install(ContentNegotiation) {
        gson {
        }
    }
    Database.connect(
        "jdbc:mysql://localhost/db_user_kotlin", driver = "com.mysql.jdbc.Driver",
        user = "root", password = ""
    )

    routing {
        get("/") {
            call.respondText("HELLO WORLD!", contentType = ContentType.Text.Plain)
        }

        get("/json/gson") {
            call.respond(mapOf("hello" to "world"))
        }
        get("/users") {
            val users = getAllUsers()
            call.respond(mapOf("data" to users))
        }
        get("/users/{id}") {
            val id = call.parameters["id"]?.toInt()
            val user = getUserById(id ?: 0)
            if (user == null) call.respond(HttpStatusCode.InternalServerError)
            else call.respond(user)
        }

        post("/users") {
            val user = call.receive<NewUser>()
            val inserted = addUser(user)
            if (inserted == null) call.respond(HttpStatusCode.InternalServerError)
            else call.respond(inserted)
        }

        put("/user/{id}") {
            val id = call.parameters["id"]?.toInt()
            val user = call.receive<NewUser>()
            val updated = updateUser(id ?: 0, user)
            if (updated == null) call.respond(HttpStatusCode.NotFound)
            else call.respond(updated)
        }

        delete("user/{id}") {
            val id = call.parameters["id"]?.toInt()
            val deleted = deleteUser(id ?: 0)
            if (deleted) call.respond(ResponseServer("Delete Success"))
            else call.respond(ResponseServer("Delete Failed"))
        }
    }
}

suspend fun deleteUser(id: Int): Boolean {
    return withContext(Dispatchers.IO) {
        transaction {
            UsersTable.deleteWhere {
                UsersTable.id eq id
            } > 0
        }
    }
}

suspend fun updateUser(id: Int, user: NewUser): User? {
    return withContext(Dispatchers.IO) {
        transaction {
            UsersTable.update({ UsersTable.id eq id }) {
                it[name] = user.name
                it[email] = user.email
                it[createdAt] = DateTime.parse(user.createdAt)
            }
        }
        return@withContext getUserById(id)
    }
}

suspend fun addUser(user: NewUser): User? {
    var userId = 0
    return withContext(Dispatchers.IO) {
        transaction {
            userId = (UsersTable.insert {
                it[name] = user.name
                it[email] = user.email
                it[createdAt] = DateTime.parse(user.createdAt)
            } get UsersTable.id)
        }
        return@withContext getUserById(userId)
    }
}

suspend fun getUserById(id: Int): User? {
    return withContext(Dispatchers.IO) {
        transaction {
            UsersTable.select { UsersTable.id eq id }.mapNotNull { it.toModel() }.singleOrNull()
        }
    }

}

suspend fun getAllUsers(): List<User> {
    return withContext(Dispatchers.IO) {
        transaction {
            UsersTable.selectAll().map {
                it.toModel()
            }
        }
    }
}


object UsersTable : Table() {
    val id = integer("id").autoIncrement()
    val name = varchar("name", 50)
    val email = varchar("email", 30)
    val createdAt = date("createdAt")

    override val primaryKey = PrimaryKey(id)
}

data class User(
    val id: Int,
    val name: String,
    val email: String,
    val createdAt: String
)

data class NewUser(
    val name: String,
    val email: String,
    val createdAt: String
)


fun ResultRow.toModel(): User {
    return User(
        this[UsersTable.id],
        this[UsersTable.name],
        this[UsersTable.email],
        this[UsersTable.createdAt].toString(),
    )
}

data class ResponseServer(
    val message: String
)
