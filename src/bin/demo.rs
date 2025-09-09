use quill_sql::database::Database;
use quill_sql::transaction::{TransactionManager, IsolationLevel};
use quill_sql::error::QuillSQLResult;

fn main() -> QuillSQLResult<()> {
    println!("🚀 QuillSQL 数据库架构和并发控制演示");
    println!("=====================================");
    
    // 演示事务管理
    demonstrate_transaction_management()?;
    
    // 演示数据库基本操作
    demonstrate_database_operations()?;
    
    println!("\n✅ 演示完成！");
    Ok(())
}

fn demonstrate_transaction_management() -> QuillSQLResult<()> {
    println!("\n📊 事务管理演示:");
    println!("-----------------");
    
    let txn_manager = TransactionManager::new();
    
    // 开始多个事务
    println!("1. 开始三个并发事务...");
    let txn1 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
    let txn2 = txn_manager.begin(IsolationLevel::SnapshotIsolation)?;
    let txn3 = txn_manager.begin(IsolationLevel::Serializable)?;
    
    println!("   事务 {} 已开始 (隔离级别: ReadUncommitted)", txn1.txn_id);
    println!("   事务 {} 已开始 (隔离级别: SnapshotIsolation)", txn2.txn_id);
    println!("   事务 {} 已开始 (隔离级别: Serializable)", txn3.txn_id);
    println!("   当前活跃事务数: {}", txn_manager.active_transaction_count()?);
    
    // 提交第一个事务
    println!("\n2. 提交事务 {}...", txn1.txn_id);
    txn_manager.commit(txn1)?;
    println!("   事务已成功提交");
    println!("   当前活跃事务数: {}", txn_manager.active_transaction_count()?);
    
    // 中止第二个事务
    println!("\n3. 中止事务 {}...", txn2.txn_id);
    txn_manager.abort(txn2)?;
    println!("   事务已中止");
    println!("   当前活跃事务数: {}", txn_manager.active_transaction_count()?);
    
    // 提交最后一个事务
    println!("\n4. 提交事务 {}...", txn3.txn_id);
    txn_manager.commit(txn3)?;
    println!("   所有事务已完成");
    println!("   当前活跃事务数: {}", txn_manager.active_transaction_count()?);
    
    Ok(())
}

fn demonstrate_database_operations() -> QuillSQLResult<()> {
    println!("\n🗄️  数据库操作演示:");
    println!("-------------------");
    
    // 创建临时数据库实例
    println!("1. 创建临时数据库实例...");
    let mut db = Database::new_temp()?;
    println!("   数据库实例创建成功");
    
    // 演示创建表
    println!("\n2. 创建表结构...");
    let create_table_sql = "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(100)
        )
    ";
    
    match db.run(create_table_sql) {
        Ok(_) => println!("   表 'users' 创建成功"),
        Err(e) => println!("   表创建失败: {}", e),
    }
    
    // 演示插入数据
    println!("\n3. 插入测试数据...");
    let insert_sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";
    
    match db.run(insert_sql) {
        Ok(_) => println!("   数据插入成功"),
        Err(e) => println!("   数据插入失败: {}", e),
    }
    
    // 演示查询数据
    println!("\n4. 查询数据...");
    let select_sql = "SELECT * FROM users";
    
    match db.run(select_sql) {
        Ok(tuples) => {
            println!("   查询成功，返回 {} 条记录", tuples.len());
            for (i, tuple) in tuples.iter().enumerate() {
                println!("   记录 {}: {:?}", i + 1, tuple);
            }
        }
        Err(e) => println!("   查询失败: {}", e),
    }
    
    // 刷新缓冲池
    println!("\n5. 刷新缓冲池到磁盘...");
    db.flush()?;
    println!("   缓冲池刷新完成");
    
    Ok(())
}