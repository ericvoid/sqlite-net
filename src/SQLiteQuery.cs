#if WINDOWS_PHONE && !USE_WP8_NATIVE_SQLITE
#define USE_CSHARP_SQLITE
#endif

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

#if USE_CSHARP_SQLITE
using Sqlite3 = Community.CsharpSqlite.Sqlite3;
using Sqlite3DatabaseHandle = Community.CsharpSqlite.Sqlite3.sqlite3;
using Sqlite3Statement = Community.CsharpSqlite.Sqlite3.Vdbe;
#elif USE_WP8_NATIVE_SQLITE
using Sqlite3 = Sqlite.Sqlite3;
using Sqlite3DatabaseHandle = Sqlite.Database;
using Sqlite3Statement = Sqlite.Statement;
#else
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;
using System.Reflection;
#endif

namespace SQLite
{
	
	public partial class SQLiteCommand
	{
		SQLiteConnection _conn;
		private List<Binding> _bindings;

		public string CommandText { get; set; }

		internal SQLiteCommand (SQLiteConnection conn)
		{
			_conn = conn;
			_bindings = new List<Binding> ();
			CommandText = "";
		}

		public int ExecuteNonQuery ()
		{
			if (_conn.Trace) {
				Debug.WriteLine ("Executing: " + this);
			}
			
			var r = SQLite3.Result.OK;
			var stmt = Prepare ();
			r = SQLite3.Step (stmt);
			Finalize (stmt);
			if (r == SQLite3.Result.Done) {
				int rowsAffected = SQLite3.Changes (_conn.Handle);
				return rowsAffected;
			} else if (r == SQLite3.Result.Error) {
				string msg = SQLite3.GetErrmsg (_conn.Handle);
				throw SQLiteException.New (r, msg);
			} else {
				throw SQLiteException.New (r, r.ToString ());
			}
		}

		public IEnumerable<T> ExecuteDeferredQuery<T> ()
		{
			return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T)));
		}

		public List<T> ExecuteQuery<T> ()
		{
			return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T))).ToList();
		}

		public List<T> ExecuteQuery<T> (TableMapping map)
		{
			return ExecuteDeferredQuery<T>(map).ToList();
		}

		/// <summary>
		/// Invoked every time an instance is loaded from the database.
		/// </summary>
		/// <param name='obj'>
		/// The newly created object.
		/// </param>
		/// <remarks>
		/// This can be overridden in combination with the <see cref="SQLiteConnection.NewCommand"/>
		/// method to hook into the life-cycle of objects.
		///
		/// Type safety is not possible because MonoTouch does not support virtual generic methods.
		/// </remarks>
		protected virtual void OnInstanceCreated (object obj)
		{
			// Can be overridden.
		}

		public IEnumerable<T> ExecuteDeferredQuery<T> (TableMapping map)
		{
			if (_conn.Trace) {
				Debug.WriteLine ("Executing Query: " + this);
			}

			var stmt = Prepare ();
			try
			{
				var cols = new Column[SQLite3.ColumnCount (stmt)];

				for (int i = 0; i < cols.Length; i++) {
					var name = SQLite3.ColumnName16 (stmt, i);
					cols [i] = map.FindColumn (name);
				}
			
				while (SQLite3.Step (stmt) == SQLite3.Result.Row) {
					var obj = Activator.CreateInstance(map.MappedType);
					for (int i = 0; i < cols.Length; i++) {
						if (cols [i] == null)
							continue;
						var colType = SQLite3.ColumnType (stmt, i);
						var val = ReadCol (stmt, i, colType, cols [i].ColumnType);
						cols [i].SetValue (obj, val);
 					}
					OnInstanceCreated (obj);
					yield return (T)obj;
				}
			}
			finally
			{
				SQLite3.Finalize(stmt);
			}
		}

		public T ExecuteScalar<T> ()
		{
			if (_conn.Trace) {
				Debug.WriteLine ("Executing Query: " + this);
			}
			
			T val = default(T);
			
			var stmt = Prepare ();

            try
            {
                var r = SQLite3.Step (stmt);
                if (r == SQLite3.Result.Row) {
                    var colType = SQLite3.ColumnType (stmt, 0);
                    val = (T)ReadCol (stmt, 0, colType, typeof(T));
                }
                else if (r == SQLite3.Result.Done) {
                }
                else
                {
                    throw SQLiteException.New (r, SQLite3.GetErrmsg (_conn.Handle));
                }
            }
            finally
            {
                Finalize (stmt);
            }
			
			return val;
		}

		public void Bind (string name, object val)
		{
			_bindings.Add (new Binding {
				Name = name,
				Value = val
			});
		}

		public void Bind (object val)
		{
			Bind (null, val);
		}

		public override string ToString ()
		{
			var parts = new string[1 + _bindings.Count];
			parts [0] = CommandText;
			var i = 1;
			foreach (var b in _bindings) {
				parts [i] = string.Format ("  {0}: {1}", i - 1, b.Value);
				i++;
			}
			return string.Join (Environment.NewLine, parts);
		}

		private Sqlite3Statement Prepare()
		{
			var stmt = SQLite3.Prepare2 (_conn.Handle, CommandText);
			BindAll (stmt);
			return stmt;
		}

		void Finalize (Sqlite3Statement stmt)
		{
			SQLite3.Finalize (stmt);
		}

		void BindAll (Sqlite3Statement stmt)
		{
			int nextIdx = 1;
			foreach (var b in _bindings) {
				if (b.Name != null) {
					b.Index = SQLite3.BindParameterIndex (stmt, b.Name);
				} else {
					b.Index = nextIdx++;
				}
				
				BindParameter (stmt, b.Index, b.Value, _conn.StoreDateTimeAsTicks);
			}
		}

		internal static IntPtr NegativePointer = new IntPtr (-1);

		internal static void BindParameter (Sqlite3Statement stmt, int index, object value, bool storeDateTimeAsTicks)
		{
			if (value == null) {
				SQLite3.BindNull (stmt, index);
			} else {
				if (value is Int32) {
					SQLite3.BindInt (stmt, index, (int)value);
				} else if (value is String) {
					SQLite3.BindText (stmt, index, (string)value, -1, NegativePointer);
				} else if (value is Byte || value is UInt16 || value is SByte || value is Int16) {
					SQLite3.BindInt (stmt, index, Convert.ToInt32 (value));
				} else if (value is Boolean) {
					SQLite3.BindInt (stmt, index, (bool)value ? 1 : 0);
				} else if (value is UInt32 || value is Int64) {
					SQLite3.BindInt64 (stmt, index, Convert.ToInt64 (value));
				} else if (value is Single || value is Double || value is Decimal) {
					SQLite3.BindDouble (stmt, index, Convert.ToDouble (value));
				} else if (value is DateTime) {
					if (storeDateTimeAsTicks) {
						SQLite3.BindInt64 (stmt, index, ((DateTime)value).Ticks);
					}
					else {
						SQLite3.BindText (stmt, index, ((DateTime)value).ToString ("yyyy-MM-dd HH:mm:ss"), -1, NegativePointer);
					}
#if !NETFX_CORE
				} else if (value.GetType().IsEnum) {
#else
				} else if (value.GetType().GetTypeInfo().IsEnum) {
#endif
					SQLite3.BindInt (stmt, index, Convert.ToInt32 (value));
                } else if (value is byte[]){
                    SQLite3.BindBlob(stmt, index, (byte[]) value, ((byte[]) value).Length, NegativePointer);
                } else if (value is Guid) {
                    SQLite3.BindText(stmt, index, ((Guid)value).ToString(), 72, NegativePointer);
                } else {
                    throw new NotSupportedException("Cannot store type: " + value.GetType());
                }
			}
		}

		class Binding
		{
			public string Name { get; set; }

			public object Value { get; set; }

			public int Index { get; set; }
		}

		object ReadCol (Sqlite3Statement stmt, int index, SQLite3.ColType type, Type clrType)
		{
			if (type == SQLite3.ColType.Null) {
				return null;
			} else {
				if (clrType == typeof(String)) {
					return SQLite3.ColumnString (stmt, index);
				} else if (clrType == typeof(Int32)) {
					return (int)SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(Boolean)) {
					return SQLite3.ColumnInt (stmt, index) == 1;
				} else if (clrType == typeof(double)) {
					return SQLite3.ColumnDouble (stmt, index);
				} else if (clrType == typeof(float)) {
					return (float)SQLite3.ColumnDouble (stmt, index);
				} else if (clrType == typeof(DateTime)) {
					if (_conn.StoreDateTimeAsTicks) {
						return new DateTime (SQLite3.ColumnInt64 (stmt, index));
					}
					else {
						var text = SQLite3.ColumnString (stmt, index);
						return DateTime.Parse (text);
					}
#if !NETFX_CORE
				} else if (clrType.IsEnum) {
#else
				} else if (clrType.GetTypeInfo().IsEnum) {
#endif
					return SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(Int64)) {
					return SQLite3.ColumnInt64 (stmt, index);
				} else if (clrType == typeof(UInt32)) {
					return (uint)SQLite3.ColumnInt64 (stmt, index);
				} else if (clrType == typeof(decimal)) {
					return (decimal)SQLite3.ColumnDouble (stmt, index);
				} else if (clrType == typeof(Byte)) {
					return (byte)SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(UInt16)) {
					return (ushort)SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(Int16)) {
					return (short)SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(sbyte)) {
					return (sbyte)SQLite3.ColumnInt (stmt, index);
				} else if (clrType == typeof(byte[])) {
					return SQLite3.ColumnByteArray (stmt, index);
				} else if (clrType == typeof(Guid)) {
                  var text = SQLite3.ColumnString(stmt, index);
                  return new Guid(text);
                } else{
					throw new NotSupportedException ("Don't know how to read " + clrType);
				}
			}
		}
	}
	
	
	/// <summary>
	/// Since the insert never changed, we only need to prepare once.
	/// </summary>
	public class PreparedSqlLiteInsertCommand : IDisposable
	{
		public bool Initialized { get; set; }

		protected SQLiteConnection Connection { get; set; }

		public string CommandText { get; set; }

		protected Sqlite3Statement Statement { get; set; }
		internal static readonly Sqlite3Statement NullStatement = default(Sqlite3Statement);

		internal PreparedSqlLiteInsertCommand (SQLiteConnection conn)
		{
			Connection = conn;
		}

		public int ExecuteNonQuery (object[] source)
		{
			if (Connection.Trace) {
				Debug.WriteLine ("Executing: " + CommandText);
			}

			var r = SQLite3.Result.OK;

			if (!Initialized) {
				Statement = Prepare ();
				Initialized = true;
			}

			//bind the values.
			if (source != null) {
				for (int i = 0; i < source.Length; i++) {
					SQLiteCommand.BindParameter (Statement, i + 1, source [i], Connection.StoreDateTimeAsTicks);
				}
			}
			r = SQLite3.Step (Statement);

			if (r == SQLite3.Result.Done) {
				int rowsAffected = SQLite3.Changes (Connection.Handle);
				SQLite3.Reset (Statement);
				return rowsAffected;
			} else if (r == SQLite3.Result.Error) {
				string msg = SQLite3.GetErrmsg (Connection.Handle);
				SQLite3.Reset (Statement);
				throw SQLiteException.New (r, msg);
			} else {
				SQLite3.Reset (Statement);
				throw SQLiteException.New (r, r.ToString ());
			}
		}

		protected virtual Sqlite3Statement Prepare ()
		{
			var stmt = SQLite3.Prepare2 (Connection.Handle, CommandText);
			return stmt;
		}

		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		private void Dispose (bool disposing)
		{
			if (Statement != NullStatement) {
				try {
					SQLite3.Finalize (Statement);
				} finally {
					Statement = NullStatement;
					Connection = null;
				}
			}
		}

		~PreparedSqlLiteInsertCommand ()
		{
			Dispose (false);
		}
	}
	
	
	public abstract class BaseTableQuery
	{
		protected class Ordering
		{
			public string ColumnName { get; set; }
			public bool Ascending { get; set; }
		}
	}
	

	public class TableQuery<T> : BaseTableQuery, IEnumerable<T>
	{
		public SQLiteConnection Connection { get; private set; }

		public TableMapping Table { get; private set; }

		Expression _where;
		List<Ordering> _orderBys;
		int? _limit;
		int? _offset;

		BaseTableQuery _joinInner;
		Expression _joinInnerKeySelector;
		BaseTableQuery _joinOuter;
		Expression _joinOuterKeySelector;
		Expression _joinSelector;
				
		Expression _selector;

		TableQuery (SQLiteConnection conn, TableMapping table)
		{
			Connection = conn;
			Table = table;
		}

		public TableQuery (SQLiteConnection conn)
		{
			Connection = conn;
			Table = Connection.GetMapping (typeof(T));
		}

		public TableQuery<U> Clone<U> ()
		{
			var q = new TableQuery<U> (Connection, Table);
			q._where = _where;
			q._deferred = _deferred;
			if (_orderBys != null) {
				q._orderBys = new List<Ordering> (_orderBys);
			}
			q._limit = _limit;
			q._offset = _offset;
			q._joinInner = _joinInner;
			q._joinInnerKeySelector = _joinInnerKeySelector;
			q._joinOuter = _joinOuter;
			q._joinOuterKeySelector = _joinOuterKeySelector;
			q._joinSelector = _joinSelector;
			q._selector = _selector;
			return q;
		}

		public TableQuery<T> Where (Expression<Func<T, bool>> predExpr)
		{
			if (predExpr.NodeType == ExpressionType.Lambda) {
				var lambda = (LambdaExpression)predExpr;
				var pred = lambda.Body;
				var q = Clone<T> ();
				q.AddWhere (pred);
				return q;
			} else {
				throw new NotSupportedException ("Must be a predicate");
			}
		}

		public TableQuery<T> Take (int n)
		{
			var q = Clone<T> ();
			q._limit = n;
			return q;
		}

		public TableQuery<T> Skip (int n)
		{
			var q = Clone<T> ();
			q._offset = n;
			return q;
		}

		public T ElementAt (int index)
		{
			return Skip (index).Take (1).First ();
		}

		bool _deferred;
		public TableQuery<T> Deferred ()
		{
			var q = Clone<T> ();
			q._deferred = true;
			return q;
		}

		public TableQuery<T> OrderBy<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, true);
		}

		public TableQuery<T> OrderByDescending<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, false);
		}

		private TableQuery<T> AddOrderBy<U> (Expression<Func<T, U>> orderExpr, bool asc)
		{
			if (orderExpr.NodeType == ExpressionType.Lambda) {
				var lambda = (LambdaExpression)orderExpr;
				
				MemberExpression mem = null;
				
				var unary = lambda.Body as UnaryExpression;
				if (unary != null && unary.NodeType == ExpressionType.Convert) {
					mem = unary.Operand as MemberExpression;
				}
				else {
					mem = lambda.Body as MemberExpression;
				}
				
				if (mem != null && (mem.Expression.NodeType == ExpressionType.Parameter)) {
					var q = Clone<T> ();
					if (q._orderBys == null) {
						q._orderBys = new List<Ordering> ();
					}
					q._orderBys.Add (new Ordering {
						ColumnName = Table.FindColumnWithPropertyName(mem.Member.Name).Name,
						Ascending = asc
					});
					return q;
				} else {
					throw new NotSupportedException ("Order By does not support: " + orderExpr);
				}
			} else {
				throw new NotSupportedException ("Must be a predicate");
			}
		}

		private void AddWhere (Expression pred)
		{
			if (_where == null) {
				_where = pred;
			} else {
				_where = Expression.AndAlso (_where, pred);
			}
		}
				
		public TableQuery<TResult> Join<TInner, TKey, TResult> (
			TableQuery<TInner> inner,
			Expression<Func<T, TKey>> outerKeySelector,
			Expression<Func<TInner, TKey>> innerKeySelector,
			Expression<Func<T, TInner, TResult>> resultSelector)
		{
			var q = new TableQuery<TResult> (Connection, Connection.GetMapping (typeof (TResult))) {
				_joinOuter = this,
				_joinOuterKeySelector = outerKeySelector,
				_joinInner = inner,
				_joinInnerKeySelector = innerKeySelector,
				_joinSelector = resultSelector,
			};
			return q;
		}
				
		public TableQuery<TResult> Select<TResult> (Expression<Func<T, TResult>> selector)
		{
			var q = Clone<TResult> ();
			q._selector = selector;
			return q;
		}

		private SQLiteCommand GenerateCommand (string selectionList)
		{
			if (_joinInner != null && _joinOuter != null) {
				throw new NotSupportedException ("Joins are not supported.");
			}
			else {
				var cmdText = "select " + selectionList + " from \"" + Table.TableName + "\"";
				var args = new List<object> ();
				if (_where != null) {
					var w = CompileExpr (_where, args);
					cmdText += " where " + w.CommandText;
				}
				if ((_orderBys != null) && (_orderBys.Count > 0)) {
					var t = string.Join (", ", _orderBys.Select (o => "\"" + o.ColumnName + "\"" + (o.Ascending ? "" : " desc")).ToArray ());
					cmdText += " order by " + t;
				}
				if (_limit.HasValue) {
					cmdText += " limit " + _limit.Value;
				}
				if (_offset.HasValue) {
					if (!_limit.HasValue) {
						cmdText += " limit -1 ";
					}
					cmdText += " offset " + _offset.Value;
				}
				return Connection.CreateCommand (cmdText, args.ToArray ());
			}
		}

		class CompileResult
		{
			public string CommandText { get; set; }

			public object Value { get; set; }
		}
		
		// TODO: Refactor this method please
		private CompileResult CompileExpr (Expression expr, List<object> queryArgs)
		{
			if (expr == null) {
				throw new NotSupportedException ("Expression is NULL");
			} else if (expr is BinaryExpression) {
				var bin = (BinaryExpression)expr;
				
				var leftr = CompileExpr (bin.Left, queryArgs);
				var rightr = CompileExpr (bin.Right, queryArgs);

				//If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
				string text;
				if (leftr.CommandText == "?" && leftr.Value == null)
					text = CompileNullBinaryExpression(bin, rightr);
				else if (rightr.CommandText == "?" && rightr.Value == null)
					text = CompileNullBinaryExpression(bin, leftr);
				else
					text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
				return new CompileResult { CommandText = text };
			} else if (expr.NodeType == ExpressionType.Call) {
				
				var call = (MethodCallExpression)expr;
				var args = new CompileResult[call.Arguments.Count];
				var obj = call.Object != null ? CompileExpr (call.Object, queryArgs) : null;
				
				for (var i = 0; i < args.Length; i++) {
					args [i] = CompileExpr (call.Arguments [i], queryArgs);
				}
				
				var sqlCall = "";
				
				if (call.Method.Name == "Like" && args.Length == 2) {
					sqlCall = "(" + args [0].CommandText + " like " + args [1].CommandText + ")";
				}
				else if (call.Method.Name == "Contains" && args.Length == 2) {
					sqlCall = "(" + args [1].CommandText + " in " + args [0].CommandText + ")";
				}
				else if (call.Method.Name == "Contains" && args.Length == 1) {
					if (call.Object != null && call.Object.Type == typeof(string)) {
						sqlCall = "(" + obj.CommandText + " like ('%' || " + args [0].CommandText + " || '%'))";
					}
					else {
						sqlCall = "(" + args [0].CommandText + " in " + obj.CommandText + ")";
					}
				}
				else if (call.Method.Name == "StartsWith" && args.Length == 1) {
					sqlCall = "(" + obj.CommandText + " like (" + args [0].CommandText + " || '%'))";
				}
				else if (call.Method.Name == "EndsWith" && args.Length == 1) {
					sqlCall = "(" + obj.CommandText + " like ('%' || " + args [0].CommandText + "))";
				}
				else if (call.Method.Name == "Equals" && args.Length == 1) {
					sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
				} else if (call.Method.Name == "ToLower") {
					sqlCall = "(lower(" + obj.CommandText + "))"; 
				} else {
					sqlCall = call.Method.Name.ToLower () + "(" + string.Join (",", args.Select (a => a.CommandText).ToArray ()) + ")";
				}
				return new CompileResult { CommandText = sqlCall };
				
			} else if (expr.NodeType == ExpressionType.Constant) {
				var c = (ConstantExpression)expr;
				queryArgs.Add (c.Value);
				return new CompileResult {
					CommandText = "?",
					Value = c.Value
				};
			} else if (expr.NodeType == ExpressionType.Convert) {
				var u = (UnaryExpression)expr;
				var ty = u.Type;
				var valr = CompileExpr (u.Operand, queryArgs);
				return new CompileResult {
					CommandText = valr.CommandText,
					Value = valr.Value != null ? ConvertTo (valr.Value, ty) : null
				};
			} else if (expr.NodeType == ExpressionType.MemberAccess) {
				var mem = (MemberExpression)expr;
				
				if (mem.Expression!=null && mem.Expression.NodeType == ExpressionType.Parameter) {
					//
					// This is a column of our table, output just the column name
					// Need to translate it if that column name is mapped
					//
					var columnName = Table.FindColumnWithPropertyName (mem.Member.Name).Name;
					return new CompileResult { CommandText = "\"" + columnName + "\"" };
				} else {
					object obj = null;
					if (mem.Expression != null) {
						var r = CompileExpr (mem.Expression, queryArgs);
						if (r.Value == null) {
							throw new NotSupportedException ("Member access failed to compile expression");
						}
						if (r.CommandText == "?") {
							queryArgs.RemoveAt (queryArgs.Count - 1);
						}
						obj = r.Value;
					}
					
					//
					// Get the member value
					//
					object val = null;
					
#if !NETFX_CORE
					if (mem.Member.MemberType == MemberTypes.Property) {
#else
					if (mem.Member is PropertyInfo) {
#endif
						var m = (PropertyInfo)mem.Member;
						val = m.GetValue (obj, null);
#if !NETFX_CORE
					} else if (mem.Member.MemberType == MemberTypes.Field) {
#else
					} else if (mem.Member is FieldInfo) {
#endif
#if SILVERLIGHT
						val = Expression.Lambda (expr).Compile ().DynamicInvoke ();
#else
						var m = (FieldInfo)mem.Member;
						val = m.GetValue (obj);
#endif
					} else {
#if !NETFX_CORE
						throw new NotSupportedException ("MemberExpr: " + mem.Member.MemberType);
#else
						throw new NotSupportedException ("MemberExpr: " + mem.Member.DeclaringType);
#endif
					}
					
					//
					// Work special magic for enumerables
					//
					if (val != null && val is System.Collections.IEnumerable && !(val is string)) {
						var sb = new System.Text.StringBuilder();
						sb.Append("(");
						var head = "";
						foreach (var a in (System.Collections.IEnumerable)val) {
							queryArgs.Add(a);
							sb.Append(head);
							sb.Append("?");
							head = ",";
						}
						sb.Append(")");
						return new CompileResult {
							CommandText = sb.ToString(),
							Value = val
						};
					}
					else {
						queryArgs.Add (val);
						return new CompileResult {
							CommandText = "?",
							Value = val
						};
					}
				}
			}
			throw new NotSupportedException ("Cannot compile: " + expr.NodeType.ToString ());
		}

		static object ConvertTo (object obj, Type t)
		{
			Type nut = Nullable.GetUnderlyingType(t);
			
			if (nut != null) {
				if (obj == null) return null;				
				return Convert.ChangeType (obj, nut);
			} else {
				return Convert.ChangeType (obj, t);
			}
		}

		/// <summary>
		/// Compiles a BinaryExpression where one of the parameters is null.
		/// </summary>
		/// <param name="parameter">The non-null parameter</param>
		private string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
		{
			if (expression.NodeType == ExpressionType.Equal)
				return "(" + parameter.CommandText + " is ?)";
			else if (expression.NodeType == ExpressionType.NotEqual)
				return "(" + parameter.CommandText + " is not ?)";
			else
				throw new NotSupportedException("Cannot compile Null-BinaryExpression with type " + expression.NodeType.ToString());
		}

		string GetSqlName (Expression expr)
		{
			var n = expr.NodeType;
			if (n == ExpressionType.GreaterThan)
				return ">"; else if (n == ExpressionType.GreaterThanOrEqual) {
				return ">=";
			} else if (n == ExpressionType.LessThan) {
				return "<";
			} else if (n == ExpressionType.LessThanOrEqual) {
				return "<=";
			} else if (n == ExpressionType.And) {
				return "&";
			} else if (n == ExpressionType.AndAlso) {
				return "and";
			} else if (n == ExpressionType.Or) {
				return "|";
			} else if (n == ExpressionType.OrElse) {
				return "or";
			} else if (n == ExpressionType.Equal) {
				return "=";
			} else if (n == ExpressionType.NotEqual) {
				return "!=";
			} else {
				throw new NotSupportedException ("Cannot get SQL for: " + n);
			}
		}
		
		public int Count ()
		{
			return GenerateCommand("count(*)").ExecuteScalar<int> ();			
		}

		public int Count (Expression<Func<T, bool>> predExpr)
		{
			return Where (predExpr).Count ();
		}

		public IEnumerator<T> GetEnumerator ()
		{
			if (!_deferred)
				return GenerateCommand("*").ExecuteQuery<T>().GetEnumerator();

			return GenerateCommand("*").ExecuteDeferredQuery<T>().GetEnumerator();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
		{
			return GetEnumerator ();
		}

		public T First ()
		{
			var query = Take (1);
			return query.ToList<T>().First ();
		}

		public T FirstOrDefault ()
		{
			var query = Take (1);
			return query.ToList<T>().FirstOrDefault ();
		}
    }

}