N::Parents {
	Name: String,
	Age: Integer,
	GrewUpIn: String
}

N::Users {
	Name: String,
	Age: Integer,
	City: String
}

E::UsersToParents {
	From: Users,
	To: Parents,
	Properties: {
	}

}

