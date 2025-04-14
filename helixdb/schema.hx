N::Users {
	Name: String,
	Age: Integer,
	City: String
}

N::Parents {
	Name: String,
	Age: Integer,
	GrewUpIn: String
}

E::UsersToParents {
	From: Users,
	To: Parents,
	Properties: {
	}

}

