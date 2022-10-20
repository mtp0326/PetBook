package edu.upenn.cis.nets2120.hw3.livy;

import java.io.Serializable;

public class MyPair<A,B> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	A left;
	B right;

	public MyPair(A l, B r) {
		left = l;
		right = r;
	}

	public A getLeft() {
		return left;
	}

	public void setLeft(A left) {
		this.left = left;
	}

	public B getRight() {
		return right;
	}

	public void setRight(B right) {
		this.right = right;
	}


	public String toString() {
		return "(" + left.toString() + ", " + right.toString() + ")";
	}
}
